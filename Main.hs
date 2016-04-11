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
-- hriak-admin count-keys
--     --host        <host>
--     --port        <port>
--     --bucket-type <bucket-type>
--     --bucket      <bucket-name>

data CountKeysArgs = CountKeysArgs Host Port BucketType BucketName
  deriving Show

countKeysParser :: Parser CountKeysArgs
countKeysParser = CountKeysArgs
  <$> optText "host"        'o' Default
  <*> optInt  "port"        'p' Default
  <*> optText "bucket-type" 't' Default
  <*> optText "bucket"      'b' Default

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
              , "query"  .= 
                [ object
                  [ "map" .= object
                    [ "language" .= String "javascript"
                    , "keep"     .= False
                    , "source"   .= String "function(o){return [1];}"
                    ]
                  ]
                , object
                  [ "reduce" .= object
                    [ "language" .= String "javascript"
                    , "keep"     .= True
                    , "name"     .= String "Riak.reduceSum"
                    ]
                  ]
                ]
              , "timeout" .= Number 1800000 -- 30 minutes
              ]
      resp <- Wreq.postWith opts url body
      print (head (read (cs (resp^.Wreq.responseBody)) :: [Int]))
