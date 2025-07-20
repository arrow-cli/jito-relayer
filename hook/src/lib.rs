use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use mev_relayer_protos::hook_proto::hook_server::{Hook, HookServer};
use mev_relayer_protos::hook_proto::{DropTransactionRequest, DropTransactionResponse, SerializedVersionedTransaction, SubscribeRequest};
use mev_relayer_protos::hook_proto::mev_hook_client::MevHookClient;
use tokio_stream::{Stream, StreamExt};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{async_trait, Request, Response, Status};
use tonic::transport::Channel;
use tokio::task::JoinHandle;
use tokio::time::error::Elapsed;

type BroadcastStream = tokio::sync::broadcast::Sender<Vec<u8>>;

pub struct MevGrpcClient {
    client: MevHookClient<Channel>
}

impl MevGrpcClient {
    pub async fn connect(url: String) -> Result<Self, tonic::transport::Error> {
        let client = MevHookClient::connect(url).await?;
        Ok(MevGrpcClient { client })
    }
    
    /// Sends a request to the MEV hook server and ask it whether to drop a transaction.
    /// 
    /// Concurrency: This method is designed to be called concurrently from multiple tasks.
    /// You need to await the returned `JoinHandle` to get the result of the request.
    pub fn request_drop_tx_response(
        &self,
        request: DropTransactionRequest,
        timeout: Duration
    ) -> JoinHandle<Result<Result<Response<DropTransactionResponse>, Status>, Elapsed>> {
        // apparently, cloning tonic clients is cheap and they share the same underlying comm buffer
        let mut client = self.client.clone();
        tokio::spawn(async move {
            tokio::time::timeout(
                timeout,
                client.drop_transaction(request)
            ).await
        })
    }
}

#[derive(Debug)]
pub struct HookServerStage {
    broadcast_stream: BroadcastStream,
    /// The authentication code required to subscribe to the hook server.
    auth_code: String,
}

impl HookServerStage {
    pub async fn new(auth_code: String, broadcaster: BroadcastStream) -> Self {
        HookServerStage {
            broadcast_stream: broadcaster,
            auth_code,
        }
    }

    pub fn to_service(self) -> HookServer<HookServerStage> {
        HookServer::new(self)
    }
}

#[async_trait]
impl Hook for HookServerStage {
    type SubscribeStream = Pin<Box<dyn Stream<Item=Result<SerializedVersionedTransaction, Status>> + Send>>;

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
