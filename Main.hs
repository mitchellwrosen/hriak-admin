{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}

module Main where

import Control.Lens ((^.), (.~))
import Data.Aeson
import Data.Optional
import Data.String.Conversions (cs)
import Turtle

import qualified Network.HTTP.Client as HTTP
import qualified Network.Wreq        as Wreq

type Host       = Text
type Port       = Int
type BucketType = Text
type BucketName = Text

data CommandArgs
  = CommandCountKeys CountKeysArgs

commandParser :: Parser CommandArgs
commandParser = 
  subcommand 
    "count-keys" 
    "Count the keys in a bucket" 
    (CommandCountKeys <$> countKeysParser)

--------------------------------------------------------------------------------
-- hriak-admin count-keys <host> <port> <type> <bucket>

data CountKeysArgs = CountKeysArgs Host Port BucketType BucketName
  deriving Show

countKeysParser :: Parser CountKeysArgs
countKeysParser = CountKeysArgs
  <$> argText "host"   (Specific "Riak host")
  <*> argInt  "port"   (Specific "Riak port")
  <*> argText "type"   (Specific "Bucket type")
  <*> argText "bucket" (Specific "Bucket name")

--------------------------------------------------------------------------------
-- main

main :: IO ()
main =
  options "Riak console client" commandParser >>= \case
    CommandCountKeys (CountKeysArgs host port bucket_type bucket) -> do
      let opts = Wreq.defaults 
                   & Wreq.manager .~ Left (HTTP.defaultManagerSettings { HTTP.managerResponseTimeout = Nothing })
          url = "http://" <> cs host <> ":" <> show port <> "/mapred"
          body = 
            object
              [ "inputs" .= [bucket_type, bucket]
              , "timeout" .= Number 86400000
              , "query"  .= 
                [ object
                  [ "map" .= object
                    [ "language"                .= String "erlang"
                    , "module"                  .= String "riak_kv_mapreduce"
                    , "function"                .= String "map_identity"                 
                    , "mapred_always_prereduce" .= True
                    ]
                  ]
                , object
                  [ "reduce" .= object
                    [ "language" .= String "erlang"
                    , "module"   .= String "riak_kv_mapreduce"
                    , "function" .= String "reduce_count_inputs"
                    , "arg"      .= object
                      [ "reduce_phase_batch_size" .= Number 1000 ]
                    ]
                  ]
                ]
              ]
      resp <- Wreq.postWith opts url body
      print (head (read (cs (resp^.Wreq.responseBody)) :: [Int]))
