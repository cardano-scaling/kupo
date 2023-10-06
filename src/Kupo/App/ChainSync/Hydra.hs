--  This Source Code Form is subject to the terms of the Mozilla Public
--  License, v. 2.0. If a copy of the MPL was not distributed with this
--  file, You can obtain one at http://mozilla.org/MPL/2.0/.

module Kupo.App.ChainSync.Hydra
    ( connect
    , runChainSyncClient
    , newTransactionStore
    , findLatestPoint
    , TransactionStore (..)
    , TransactionStoreException (..)
    ) where

import Kupo.Prelude

import Control.Exception.Safe
    ( MonadThrow
    , throwM
    )
import qualified Data.Map as Map
import Kupo.App.Mailbox
    ( Mailbox
    , putHighFrequencyMessage
    )
import Kupo.Control.MonadSTM
    ( MonadSTM (..)
    )
import Kupo.Data.Cardano
    ( Point
    , SlotNo (..)
    , Tip
    , TransactionId
    , getPointSlotNo
    )
import Kupo.Data.ChainSync
    ( IntersectionNotFoundException (..)
    )
import Kupo.Data.Hydra
    ( HydraMessage (..)
    , Snapshot (..)
    , decodeHydraMessage
    , mkHydraBlock
    )
import Kupo.Data.PartialBlock
    ( PartialBlock
    , PartialTransaction (..)
    )
import qualified Network.WebSockets as WS
import qualified Network.WebSockets.Json as WS

runChainSyncClient
    :: forall m.
        ( MonadIO m
        , MonadSTM m
        , MonadThrow m, MonadThrow (STM m))
    => Mailbox m (Tip, PartialBlock) (Tip, Point)
    -> m () -- An action to run before the main loop starts.
    -> [Point]
    -> WS.Connection
    -> m IntersectionNotFoundException
runChainSyncClient mailbox beforeMainLoop pts ws = do
    beforeMainLoop
    txStore <- newTransactionStore
    let mLatestPoint = findLatestPoint pts
    forever $ do
        WS.receiveJson ws decodeHydraMessage >>= \case
            HeadIsOpen{genesisTxs} ->
                atomically (putHighFrequencyMessage mailbox (mkHydraBlock 0 genesisTxs))
            TxValid{tx} ->
                -- TODO: Ideally we would not push txs into the store unless they come after last
                -- recorded 'Point' but we don't have such knowledge at this code path since 'TxValid'
                -- only contains 'PartialTransaction' which doesn't hold the notion of
                -- blocks/slots. Maybe add current snapshot number to 'TxValid' in hydra-node or bring
                -- back the transactions inside of 'SnapshotConfirmed' like what we had before?
                pushTx txStore tx
            SnapshotConfirmed{ snapshot = snapshot@Snapshot { number }} -> do
                case mLatestPoint of
                  Nothing -> sendToMailbox txStore snapshot
                  Just lastRecordedPoint ->
                     when (getPointSlotNo lastRecordedPoint < SlotNo number) $
                         sendToMailbox txStore snapshot
            SomethingElse -> pure ()
    where
        sendToMailbox TransactionStore{popTxByIds} Snapshot { number, confirmedTransactionIds } = do
           txs <- popTxByIds confirmedTransactionIds
           atomically (putHighFrequencyMessage mailbox (mkHydraBlock number txs))

-- | Potentially find the latest recorded 'Point' in a list. NOTE: blocks/slots are just snapshot
-- numbers when it comes to Hydra Head protocol so this is why we can use 'getPointSlotNo' for
-- comparison.
findLatestPoint :: [Point] -> Maybe Point
findLatestPoint = \case
    [] -> Nothing
    as -> listToMaybe $
             sortBy (\a b -> compare (getPointSlotNo a) (getPointSlotNo b) ) as

connect
    :: ConnectionStatusToggle IO
    -> String
    -> Int
    -> (WS.Connection -> IO a)
    -> IO a
connect ConnectionStatusToggle{toggleConnected} host port action =
    WS.runClientWith host port "/"
        WS.defaultConnectionOptions [] (\ws -> toggleConnected >> action ws)

newtype TransactionStoreException = TransactionNotInStore { transactionId :: TransactionId }
  deriving (Eq, Show)

instance Exception TransactionStoreException

-- | Handle to store and later retrieve transaction.
data TransactionStore m = TransactionStore
    { -- | Store a transaction for later retrieval.
      pushTx :: PartialTransaction -> m ()
    , -- | Resolves a transaction id and removes it. Throws
      -- 'TransactionNotInStore' when not found.
      popTxByIds :: MonadThrow m => [TransactionId] -> m [PartialTransaction]
    }

newTransactionStore :: (Monad m, MonadSTM m, MonadThrow (STM m)) => m (TransactionStore m)
newTransactionStore = do
  txStore <- atomically $ newTVar mempty
  pure
    TransactionStore
        { pushTx = \tx@PartialTransaction{id} -> atomically $ modifyTVar' txStore (Map.insert id tx)
        , popTxByIds = \txIds ->
            atomically $ do
                txMap <- readTVar txStore
                forM txIds $ \txId ->
                  case Map.lookup txId txMap of
                    Nothing -> throwM $ TransactionNotInStore txId
                    Just tx -> do
                      writeTVar txStore (Map.delete txId txMap)
                      pure tx
        }



