"use server"

import { KinesisVideoClient, KinesisVideoClientConfig, DescribeSignalingChannelCommand, GetSignalingChannelEndpointCommandInput, GetSignalingChannelEndpointCommand } from "@aws-sdk/client-kinesis-video";
import { KinesisVideoSignalingClient, GetIceServerConfigCommand } from "@aws-sdk/client-kinesis-video-signaling"; // ES Modules import

const accessKeyId = process.env.AWS_ACCESS_KEY_ID || "";
const secretAccessKey = process.env.AWS_SECRET_ACCESS_KEY || "";
const region = process.env.AWS_REGION;

export async function kvsConnect(role:string) {
  try {
    const { channelARN, endpoints } = await getEndpointsAndChannelARN(role);
    const iceServers = await getIceServers(channelARN, endpoints);

    return { iceServers: iceServers as RTCIceServer[], channelARN, endpoints }
  } catch (error) {
    console.error("[ERROR kvs-connect]: ", error)
    throw new Error("kvs-connect problem")
  }
}

async function getIceServers(channelARN: any, endpoints: any) {
  const kinesisVideoSignalingClient = new KinesisVideoSignalingClient({
    region,
    credentials: {
      accessKeyId,
      secretAccessKey
    },
    endpoint: endpoints ? endpoints[0].ResourceEndpoint : ""
  })

  const getIceServerConfigResponse = await kinesisVideoSignalingClient
    .send(
      new GetIceServerConfigCommand({
        ChannelARN: channelARN,
        Service: "TURN",
      })
    )

  const iceServers = [
    { urls: `stun:stun.kinesisvideo.${region}.amazonaws.com:443` },
    ...(getIceServerConfigResponse.IceServerList || []).map(iceServer => ({
      urls: iceServer.Uris,
      username: iceServer.Username,
      credential: iceServer.Password,
    }))
  ]

  return iceServers;
}

async function getEndpointsAndChannelARN(role: string) {
  const kinesisVideoClientConfig = {
    region: process.env.AWS_REGION,
    credentials: {
      accessKeyId: process.env.AWS_ACCESS_KEY_ID || "",
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
    }
  } as KinesisVideoClientConfig;

  const kinesisVideoClient = new KinesisVideoClient(kinesisVideoClientConfig);
  const descSigChanInput = { ChannelName: "tech-demo-room-123" }
  const descSigChanCommand = new DescribeSignalingChannelCommand(descSigChanInput)
  const descSigChanResponse = await kinesisVideoClient.send(descSigChanCommand);

  const getSinChanEndpointInput = { // GetSignalingChannelEndpointInput
    ChannelARN: descSigChanResponse.ChannelInfo?.ChannelARN, // required
    SingleMasterChannelEndpointConfiguration: { // SingleMasterChannelEndpointConfiguration
      Protocols: [ // ListOfProtocols
        "HTTPS", "WSS",
      ],
      Role: role,
    },
  } as GetSignalingChannelEndpointCommandInput;

  const getSigChanEndpointCommand = new GetSignalingChannelEndpointCommand(getSinChanEndpointInput);
  const getSigChanEndpointResponse = await kinesisVideoClient.send(getSigChanEndpointCommand);

  return { channelARN: descSigChanResponse.ChannelInfo?.ChannelARN, endpoints: getSigChanEndpointResponse.ResourceEndpointList }
} 