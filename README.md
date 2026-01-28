# AWS Kinesis Video Streams WebRTC with Ingestion and Storage

A Next.js application that demonstrates real-time video streaming using AWS Kinesis Video Streams (KVS) WebRTC with ingestion and storage capabilities. This is a reimplementation of the [amazon-kinesis-video-streams-webrtc-sdk-js demo](https://awslabs.github.io/amazon-kinesis-video-streams-webrtc-sdk-js/examples/index.html) built for Next.js.

## Features

- **Master Mode**: Sends video/audio streams and handles storage session management
- **Viewer Mode**: Receives video/audio streams from master and participates in storage sessions
- **WebRTC Ingestion**: Real-time media streaming with AWS KVS
- **Storage Integration**: Automatic recording of WebRTC sessions to Kinesis Video Streams
- **ICE Server Configuration**: Automatic STUN/TURN server setup via AWS

## Prerequisites

- Node.js 18+ and pnpm
- AWS Account with appropriate permissions
- AWS CLI configured (optional, for setup)

## AWS Setup

### 1. Create IAM User and Policy

Create an IAM user with the following policy for KVS WebRTC operations:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "kinesisvideo:DescribeSignalingChannel",
                "kinesisvideo:GetSignalingChannelEndpoint",
                "kinesisvideo:CreateSignalingChannel",
                "kinesisvideo:GetDataEndpoint",
                "kinesisvideo:PutMedia",
                "kinesisvideo:GetMedia",
                "kinesisvideo:ListSignalingChannels"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "kinesisvideo-webrtc-storage:JoinStorageSession",
                "kinesisvideo-webrtc-storage:JoinStorageSessionAsViewer"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "kinesisvideosignaling:GetIceServerConfig",
                "kinesisvideosignaling:SendAlexaOfferToMaster",
                "kinesisvideosignaling:ConnectAsMaster",
                "kinesisvideosignaling:ConnectAsViewer"
            ],
            "Resource": "*"
        }
    ]
}
```

### 2. Create Signaling Channel

Create a signaling channel named `tech-demo-room-123`:

**Using AWS CLI:**
```bash
aws kinesisvideo create-signaling-channel \
    --channel-name tech-demo-room-123 \
    --region us-east-1
```

**Using AWS Console:**
1. Go to Amazon Kinesis Video Streams console
2. Navigate to "Signaling channels"
3. Click "Create signaling channel"
4. Enter name: `tech-demo-room-123`
5. Click "Create channel"

### 3. Create Video Stream

**Using AWS Console:**
1. Go to Amazon Kinesis Video Streams console
2. Navigate to "Video streams"
3. Click "Create video stream"
4. Enter name: `demo-123`
5. Click "Create video stream"

### 4. Enable WebRTC Ingestion and Storage

**Using the AWS KVS WebRTC Demo Website:**

1. Go to [https://awslabs.github.io/amazon-kinesis-video-streams-webrtc-sdk-js/examples/index.html](https://awslabs.github.io/amazon-kinesis-video-streams-webrtc-sdk-js/examples/index.html)

2. **Configure the demo:**
   - Enter your AWS credentials (Access Key ID and Secret Access Key)
   - Set Region to your AWS region (e.g., `us-east-1`)
   - Set Channel Name to `tech-demo-room-123`
   - Click `WebRTC Ingestion and Storage` dropdown
   - Enter the name of the Video Stream
   - Click `Update Media Storage Configuration`

**Note:** The AWS KVS WebRTC demo website handles the complex setup of ingestion and storage automatically. There's currently no direct way to enable this through the AWS Console for WebRTC storage.

## Installation

1. **Clone the repository:**
```bash
git clone <repository-url>
cd aws-webrtc-kvs-next
```

2. **Install dependencies:**
```bash
pnpm install
```

3. **Set up environment variables:**
```bash
cp .sample.env .env
```

Edit `.env` with your AWS credentials:
```env
AWS_REGION=us-east-1
NEXT_PUBLIC_AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=your_access_key_id
AWS_SECRET_ACCESS_KEY=your_secret_access_key
NEXT_PUBLIC_AWS_ACCESS_KEY_ID=your_access_key_id
NEXT_PUBLIC_AWS_SECRET_ACCESS_KEY=your_secret_access_key
```

## Usage

### Development

Start the development server:
```bash
pnpm dev
```

Open [http://localhost:3000](http://localhost:3000) in your browser.

### Production

Build and start the production server:
```bash
pnpm build
pnpm start
```

## How to Use

### 1. Master Mode
1. Navigate to [http://localhost:3000](http://localhost:3000)
2. Click "Master"
3. Click "Start Storage" to begin streaming
4. The master will:
   - Connect to the signaling channel
   - Join the storage session
   - Wait for viewers to connect
   - Stream video/audio to connected viewers
   - Record the session to Kinesis Video Streams

### 2. Viewer Mode
1. Open a new browser tab/window
2. Navigate to [http://localhost:3000](http://localhost:3000)
3. Click "Viewer"
4. Click "Start Storage" to connect
5. The viewer will:
   - Connect to the signaling channel
   - Join the storage session as a viewer
   - Receive video/audio from the master
   - Display the remote stream

### 3. Testing the Connection
1. Start Master in one browser tab
2. Start Viewer in another browser tab
3. Both should connect and establish WebRTC connection
4. Viewer should display video from Master
5. Session will be recorded to AWS KVS

## Architecture

```
┌─────────────┐    ┌──────────────────┐    ┌─────────────┐
│   Master    │◄──►│  AWS KVS WebRTC  │◄──►│   Viewer    │
│  (Sender)   │    │ Signaling Channel │    │ (Receiver)  │
└─────────────┘    └──────────────────┘    └─────────────┘
       │                     │                     │
       └─────────────────────┼─────────────────────┘
                             │
                    ┌────────▼────────┐
                    │ KVS Storage     │
                    │ (Recording)     │
                    └─────────────────┘
```

## Key Components

- **`app/lib/kvs-connect.ts`**: Handles AWS KVS connection, ICE servers, and endpoints
- **`app/lib/join-storage.ts`**: Manages storage session joining for both master and viewer
- **`app/master/page.tsx`**: Master interface for sending streams
- **`app/viewer/page.tsx`**: Viewer interface for receiving streams

## Configuration

The application uses a hardcoded signaling channel name `tech-demo-room-123`. To use a different channel:

1. Update the channel name in `app/lib/kvs-connect.ts`:
```typescript
const descSigChanInput = { ChannelName: "your-channel-name" }
```

2. Update the channel name in `app/lib/join-storage.ts`:
```typescript
const descSigChanInput = { ChannelName: "your-channel-name" }
```

## Troubleshooting

### Common Issues

1. **"Invalid request body" error**: Ensure your IAM user has the correct permissions for WebRTC storage operations.

2. **Connection timeout**: Check that your signaling channel exists and WebRTC ingestion is enabled.

3. **No video display**: Verify that both master and viewer are using the same signaling channel and that WebRTC connection is established.

4. **ICE connection failed**: Ensure STUN/TURN servers are properly configured and accessible.

### Debug Tips

- Check browser console for WebRTC connection logs
- Verify AWS credentials and permissions
- Ensure signaling channel exists in the correct region
- Check network connectivity for WebRTC traffic

## References

- [AWS Kinesis Video Streams WebRTC Developer Guide](https://docs.aws.amazon.com/kinesisvideostreams-webrtc-dg/)
- [Amazon Kinesis Video Streams WebRTC SDK for JavaScript](https://github.com/awslabs/amazon-kinesis-video-streams-webrtc-sdk-js)
- [WebRTC API Documentation](https://developer.mozilla.org/en-US/docs/Web/API/WebRTC_API)

## License

This project is licensed under the MIT License.