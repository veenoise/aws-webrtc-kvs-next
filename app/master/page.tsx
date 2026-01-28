'use client'

import { useRef, useState } from 'react';
import { SignalingClient, Role } from 'amazon-kinesis-video-streams-webrtc';
import { joinStorage } from '../lib/join-storage';
import { kvsConnect } from '../lib/kvs-connect';

const page = () => {
  const videoRef = useRef<HTMLVideoElement | null>(null);
  const pcRef = useRef<RTCPeerConnection | null>(null);
  const [connected, setConnected] = useState(false);
  const [status, setStatus] = useState<'idle' | string>('idle');
  const [channelArn, setChannelArn] = useState('');
  const [clientId, setClientId] = useState('master-' + Math.random().toString(36).slice(2, 8));
  const sdpOfferReceivedRef = useRef(false)
  const remoteClientIdRef = useRef('')
  const sdpOfferRef = useRef<RTCSessionDescriptionInit>(null)
  const websocketOpenedRef = useRef(false)
  const signalingClientRef = useRef<SignalingClient>(null)

  async function startStorage() {
    setStatus('Connecting...');
    try {
      const { iceServers, channelARN, endpoints } = await kvsConnect("MASTER")

      if (!(iceServers && channelARN && endpoints)) {
        throw new Error("iceServers, channelARN, or/and endpoints is/are empty")
      }

      setStatus('Fetched ICE servers');

      const signalingClient = new SignalingClient({
        channelARN,
        channelEndpoint: endpoints[1].ResourceEndpoint ?? "",
        role: Role.MASTER,
        region: process.env.NEXT_PUBLIC_AWS_REGION || "ap-southeast-1",
        credentials: {
          accessKeyId: process.env.NEXT_PUBLIC_AWS_ACCESS_KEY_ID || "",
          secretAccessKey: process.env.NEXT_PUBLIC_AWS_SECRET_ACCESS_KEY || "",
        },
      });

      signalingClientRef.current = signalingClient

      // As MASTER, create and send offer (not wait for one)
      signalingClient.on('open', async () => {
        setStatus('Signaling open');
        websocketOpenedRef.current = true
        console.log(`[MASTER] Connected to signaling service`);

        await connectToMediaServer()
      });

      // Handle answer from viewer (storage service)
      signalingClient.on('sdpOffer', async (offer, remoteClientId) => {
        const role = 'MASTER'
        console.log(`[MASTER] Received SDP offer from`, remoteClientId || 'remote');
        setStatus('Received SDP offer');
        sdpOfferReceivedRef.current = true
        sdpOfferRef.current = offer
        remoteClientIdRef.current = remoteClientId
        console.log('SDP offer:', offer);

        const pc = new RTCPeerConnection({ iceServers: iceServers as RTCIceServer[], iceTransportPolicy: 'all' });
        pcRef.current = pc;

        await pc.setRemoteDescription(offer)

        signalingClient.on('iceCandidate', async (candidate, remoteClientIdIce) => {
          if (remoteClientIdIce !== remoteClientId) {
            return;
          }
          console.log(`Received ICE candidate from ${remoteClientId || 'remote'}`);

          const inboundIceCandidateFilterFn = (candidate: RTCIceCandidate) => true;

          if (inboundIceCandidateFilterFn(candidate)) {
            pc.addIceCandidate(candidate)
          } else {
            console.log(`Candidate rejected through filter. Not adding candidate from ${remoteClientIdIce || 'remote'}.`);
          }
        })

        pc.onicecandidate = ({ candidate }) => {
          if (candidate && candidate.candidate) {
            console.log('Generated ICE candidate for', remoteClientId || 'remote');
            console.debug('ICE candidate:', candidate);

            // When trickle ICE is enabled, send the ICE candidates as they are generated.
            console.log('Sending ICE candidate to', remoteClientId || 'remote');
            signalingClient.sendIceCandidate(candidate, remoteClientId);
          }
        };

        pc.ontrack = (event) => {
          console.log(
            'Received',
            event.track.kind || 'unknown',
            'track from',
            remoteClientIdRef.current || 'remote',
            'in mediaStream:',
            event?.streams[0]?.id ?? '[Error retrieving stream ID]',
            'with track id:',
            event.track.id,
          );
        };

        const localStream = await navigator.mediaDevices.getUserMedia({
          video: { width: { ideal: 1280 }, height: { ideal: 720 } },
          audio: true,
        });

        if (localStream) {
          localStream.getTracks().forEach(track => pc.addTrack(track, localStream));
          if (videoRef.current) {
            videoRef.current.srcObject = localStream;
          }
          setStatus('Local video previewed');
        }

        const [videoCodecs, audioCodecs] = getCodecFilters();
        pc.getTransceivers().map(async (transceiver) => {
          if (transceiver.receiver.track.kind === 'video' && videoCodecs) {
            transceiver.setCodecPreferences(videoCodecs);
          } else if (transceiver.receiver.track.kind === 'audio' && audioCodecs) {
            transceiver.setCodecPreferences(audioCodecs);
          }
        })

        console.log('Creating SDP answer for', remoteClientId || 'remote');

        await pc.setLocalDescription(
          await pc.createAnswer({
            offerToReceiveAudio: true,
            offerToReceiveVideo: true,
          }),
        );

        console.log('Sending SDP answer to', remoteClientId || 'remote');
        const correlationId = Date.now().toString();
        console.debug('SDP answer:', pc.localDescription, 'correlationId:', correlationId);
        signalingClient.sendSdpAnswer(pc.localDescription!, remoteClientId, correlationId);

        console.log('Generating ICE candidates for', remoteClientId || 'remote');

        pc.onconnectionstatechange = (event) => {
          let rtcPeerConnection: RTCPeerConnection | null = null;

          if (event.target) {
            rtcPeerConnection = event.target as RTCPeerConnection;
          }

          if (rtcPeerConnection && rtcPeerConnection.connectionState === "connected") {
            console.log(
              `[MASTER] Successfully joined the storage session. Media is being recorded to`,
              'Kinesis Video Streams',
            );
          }
        };

        const CHECK_INTERVAL_SECONDS = 5;
        const RETRY_TIMEOUT_SECONDS = 10;

        for (let i = CHECK_INTERVAL_SECONDS; i <= RETRY_TIMEOUT_SECONDS; i += CHECK_INTERVAL_SECONDS) {
          setTimeout(function () {
            // check the state each 5 seconds
            //enter retry if still connecting after 30 seconds
            if (
              pc.connectionState !== 'connected' &&
              pc.connectionState !== 'failed' &&
              pc.connectionState !== 'closed'
            ) {
              if (i < RETRY_TIMEOUT_SECONDS) {
                console.log(`[${role}] Waiting for connection, time out in ${RETRY_TIMEOUT_SECONDS - i} seconds.`);
              } else {
                console.error(`[${role}] Connection failed after ${RETRY_TIMEOUT_SECONDS} seconds, will enter retry.`);
                onPeerConnectionFailed();
              }
            }
          }, i * 1000);
        }
      });

      signalingClient.on('close', () => {
        setStatus('Signaling closed');
        setConnected(false);
      });

      signalingClient.on('error', (error: any) => {
        setStatus('Signaling error');
        setConnected(false);
        console.error(error);
      });

      signalingClient.open();
    } catch (err) {
      setStatus('Connection/storage failed');
      setConnected(false);
      console.error(err);
    }
  }

  async function callJoinStorageSessionUntilSDPOfferReceived() {
    while (!sdpOfferReceivedRef.current) {
      await joinStorage()
      await new Promise(resolve => setTimeout(resolve, 6000))
    }

    return true
  }

  async function connectToMediaServer() {
    const role = 'MASTER';
    console.log(`[${role}]`, `Joining storage session...`);
    const success = await callJoinStorageSessionUntilSDPOfferReceived();
    if (success) {
      console.log(`[${role}]`, `Join storage session API call(s) completed.`);
    }
  }

  function getCodecFilters() {
    const videoCapabilities = RTCRtpSender.getCapabilities('video');
    const audioCapabilities = RTCRtpSender.getCapabilities('audio');
    const allVCodecs = videoCapabilities && videoCapabilities.codecs ? videoCapabilities.codecs : [];
    const allACodecs = audioCapabilities && audioCapabilities.codecs ? audioCapabilities.codecs : [];
    const role = "MASTER";

    /** @type {string[]} */
    const selectedVideoMimeTypes = ["video/H264"]

    /** @type {RTCRtpCodec[]} */
    const filteredVideoCodecs = selectedVideoMimeTypes.flatMap((mimeType) => {
      return allVCodecs.filter((codec) => codec.mimeType === mimeType);
    });

    /** @type {string[]} */
    const selectedAudioMimeTypes = ['audio/opus']

    /** @type {RTCRtpCodec[]} */
    const filteredAudioCodecs = selectedAudioMimeTypes.flatMap((mimeType) => {
      return allACodecs.filter((codec) => codec.mimeType === mimeType);
    });

    console.log(
      `[${role}]`,
      `Filters: Video: ${selectedVideoMimeTypes.length ? selectedVideoMimeTypes : 'No filter'}, Audio: ${selectedAudioMimeTypes.length ? selectedAudioMimeTypes : 'No filter'}`,
    );

    console.debug(
      `[${role}]`,
      `All accepted codecs: Video:`,
      filteredVideoCodecs.length ? filteredVideoCodecs : 'ALL',
      'Audio:',
      filteredAudioCodecs.length ? filteredAudioCodecs : 'ALL',
    );

    return [filteredVideoCodecs, filteredAudioCodecs];
  }

  function onPeerConnectionFailed() {
    const role = "MASTER";
    console.warn(`[${role}] Reconnecting...`);

    sdpOfferReceivedRef.current = false;
    if (!websocketOpenedRef.current) {
      console.log(`[${role}] Websocket is closed. Reopening...`);

      if (signalingClientRef.current)

        signalingClientRef.current.open();
    } else {
      connectToMediaServer();
    }
  }

  return (
    <div style={{ padding: 20 }}>
      <h1>Master — KVS WebRTC Storage</h1>

      <div style={{ marginBottom: 12 }}>
        <label style={{ display: 'block', marginBottom: 6 }}>Channel ARN</label>
        <input value={channelArn} onChange={(e) => setChannelArn(e.target.value)} placeholder="arn:aws:kinesisvideo:...:channel/your-channel/1" style={{ width: '100%', maxWidth: 960 }} />
      </div>

      <div style={{ marginBottom: 12 }}>
        <label style={{ display: 'block', marginBottom: 6 }}>Client ID</label>
        <input value={clientId} onChange={(e) => setClientId(e.target.value)} style={{ width: '100%', maxWidth: 400 }} />
      </div>

      <video
        ref={videoRef}
        autoPlay
        playsInline
        muted
        controls
        style={{ width: '100%', maxWidth: 960, background: '#000' }}
      />

      <div style={{ marginTop: 12, display: 'flex', gap: 12, alignItems: 'center' }}>
        <button onClick={startStorage} disabled={connected}>Start Storage</button>
        <div>Status: {status}</div>
      </div>
    </div>
  );
};

export default page;