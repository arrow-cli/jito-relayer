use std::pin::Pin;
use mev_relayer_protos::hook_proto::hook_server::{Hook, HookServer};
use mev_relayer_protos::hook_proto::{SerializedVersionedTransaction, SubscribeRequest};
use tokio_stream::{Stream, StreamExt};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{async_trait, IntoRequest, Request, Response, Status};

type BroadcastStream = tokio::sync::broadcast::Sender<Vec<u8>>;

#[derive(Debug)]
pub struct GrpcServer {
    broadcast_stream: BroadcastStream,
    auth_code: String,
}

impl GrpcServer {
    pub fn new(auth_code: String, broadcaster: BroadcastStream) -> Self {
        GrpcServer {
            broadcast_stream: broadcaster,
            auth_code,
        }
    }

    pub fn to_service(self) -> HookServer<GrpcServer> {
        HookServer::new(self)
    }
}

#[async_trait]
impl Hook for GrpcServer {
    type SubscribeStream = Pin<Box<dyn Stream<Item = Result<SerializedVersionedTransaction, Status>> + Send>>;

    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        if request.get_ref().auth_code != self.auth_code {
            return Err(Status::unauthenticated("Invalid auth code"));
        }

        let mut broadcast_stream = self.broadcast_stream.subscribe();

        let (tx, rx) = tokio::sync::mpsc::channel(128);

        tokio::spawn(async move {
            while let Ok(message) = broadcast_stream.recv().await {
                if tx
                    .send(SerializedVersionedTransaction { content: message })
                    .await
                    .is_err()
                {
                    // client has disconnected, stop this task by exiting the loop
                    break;
                }
            }
        });

        let stream = ReceiverStream::new(rx).map(Ok);
        Ok(Response::new(Box::pin(stream)))
    }
}
