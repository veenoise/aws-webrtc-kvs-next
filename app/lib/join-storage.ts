"use server"

import {
  KinesisVideoClient,
  KinesisVideoClientConfig,
  DescribeSignalingChannelCommand,
  GetSignalingChannelEndpointCommand,
  GetSignalingChannelEndpointCommandInput,
} from "@aws-sdk/client-kinesis-video";
import {
  KinesisVideoWebRTCStorageClient,
  JoinStorageSessionCommand,
  JoinStorageSessionInput,
  KinesisVideoWebRTCStorageClientConfig
} from "@aws-sdk/client-kinesis-video-webrtc-storage";

export async function joinStorage() {
  const { channelARN, webRTCEndpoint } = await getWebRTCEndpointAndChannelARN()

  const kinesisVideoWebRTCStorageClientConfig = {
    region: process.env.AWS_REGION,
    credentials: {
      accessKeyId: process.env.AWS_ACCESS_KEY_ID || "",
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
    },
    endpoint: webRTCEndpoint
  } as KinesisVideoWebRTCStorageClientConfig

  const kinesisVideoWebRTCStorageClient = new KinesisVideoWebRTCStorageClient(kinesisVideoWebRTCStorageClientConfig);
  const joinStorageSessionInput = { // JoinStorageSessionInput
    channelArn: channelARN
  } as JoinStorageSessionInput

  const joinStorageSessionCommand = new JoinStorageSessionCommand(joinStorageSessionInput);

  try {
    await kinesisVideoWebRTCStorageClient.send(joinStorageSessionCommand)
  } catch (err) {
    console.error("Error joining storage session: ", err);
    throw new Error("Error joining storage session")
  }
}

async function getWebRTCEndpointAndChannelARN() {
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
        "WEBRTC",
      ],
      Role: "MASTER",
    },
  } as GetSignalingChannelEndpointCommandInput;

  const getSigChanEndpointCommand = new GetSignalingChannelEndpointCommand(getSinChanEndpointInput);
  const getSigChanEndpointResponse = await kinesisVideoClient.send(getSigChanEndpointCommand);

  let webRTCEndpoint

  if (getSigChanEndpointResponse.ResourceEndpointList) {
    webRTCEndpoint = getSigChanEndpointResponse.ResourceEndpointList[0].ResourceEndpoint
  }

  return { channelARN: descSigChanResponse.ChannelInfo?.ChannelARN, webRTCEndpoint }
} 