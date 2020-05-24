%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_kafka_hook).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

-define(APP, emqx_kafka_hook).

-logger_header("[KafkaHook]").

%% APIs
-export([ register_metrics/0
        , load/1
        , unload/0
        ]).

%% Hooks callback
-export([ on_client_connected/3
        , on_client_disconnected/4
        ]).

-export([ on_session_created/3
        , on_session_subscribed/4
        , on_session_unsubscribed/4
        , on_session_terminated/4
        ]).

-export([ on_message_publish/2
        , on_message_acked/3
        ]).


%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

register_metrics() ->
    lists:foreach(fun emqx_metrics:new/1, ['kafkahook.client_connected',
                                           'kafkahook.client_disconnected',
                                           'kafkahook.session_created',
                                           'kafkahook.session_subscribed',
                                           'kafkahook.session_unsubscribed',
                                           'kafkahook.session_terminated',
                                           'kafkahook.message_publish',
                                           'kafkahook.message_acked',
                                           'kafkahook.kafka_publish']).

load(Env) ->
    brod_init(),
    emqx:hook('client.connected',    {?MODULE, on_client_connected, [Env]}),
    emqx:hook('client.disconnected', {?MODULE, on_client_disconnected, [Env]}),
    emqx:hook('session.created',     {?MODULE, on_session_created, [Env]}),
    emqx:hook('session.subscribed',  {?MODULE, on_session_subscribed, [Env]}),
    emqx:hook('session.unsubscribed',{?MODULE, on_session_unsubscribed, [Env]}),
    emqx:hook('session.terminated',  {?MODULE, on_session_terminated, [Env]}),
    emqx:hook('message.publish',     {?MODULE, on_message_publish, [Env]}),
    emqx:hook('message.acked',       {?MODULE, on_message_acked, [Env]}).

unload() ->
    emqx:unhook('client.connected',    {?MODULE, on_client_connected}),
    emqx:unhook('client.disconnected', {?MODULE, on_client_disconnected}),
    emqx:unhook('session.created',     {?MODULE, on_session_created}),
    emqx:unhook('session.subscribed',  {?MODULE, on_session_subscribed}),
    emqx:unhook('session.unsubscribed',{?MODULE, on_session_unsubscribed}),
    emqx:unhook('session.terminated',  {?MODULE, on_session_terminated}),
    emqx:unhook('message.publish',     {?MODULE, on_message_publish}),
    emqx:unhook('message.acked',       {?MODULE, on_message_acked}).



%%--------------------------------------------------------------------
%% Client Lifecircle Hooks
%%--------------------------------------------------------------------

on_client_connected(#{clientid := ClientId, username := Username}, _ConnInfo, _Env) ->
    emqx_metrics:inc('kafkahook.client_connected'),
    WorkerName = application:get_env(?APP, workername, undefined),
    Params = #{ type => connected
              , clientid => ClientId
              , username => Username
              , workername => list_to_binary(WorkerName)
              , ts => erlang:system_time(millisecond)
              },
    produce_kafka_payload(ClientId, Params),
    ok.

on_client_disconnected(ClientInfo, {shutdown, Reason}, ConnInfo, Env) when is_atom(Reason) ->
    on_client_disconnected(ClientInfo, Reason, ConnInfo, Env);
on_client_disconnected(#{clientid := ClientId, username := Username}, Reason, _ConnInfo, _Env) ->
    emqx_metrics:inc('kafkahook.client_disconnected'),
    WorkerName = application:get_env(?APP, workername, undefined),
    Params = #{ type => disconnected
              , clientid => ClientId
              , username => Username
              , reason => stringfy(Reason)
              , workername => list_to_binary(WorkerName)
              , ts => erlang:system_time(millisecond)
              },
    produce_kafka_payload(ClientId, Params),
    ok.


%%--------------------------------------------------------------------
%% Session Lifecircle Hooks
%%--------------------------------------------------------------------

on_session_created(#{clientid := ClientId, username := Username}, _SessInfo, _Env) ->
    emqx_metrics:inc('kafkahook.session_created'),
    WorkerName = application:get_env(?APP, workername, undefined),
    Params = #{ type => session_created
              , clientid => ClientId
              , username => Username
              , workername => list_to_binary(WorkerName)
              , ts => erlang:system_time(millisecond)
              },
    produce_kafka_payload(ClientId, Params),
    ok.

on_session_subscribed(#{clientid := ClientId, username := Username}, Topic, _Opts, _Env) ->
    emqx_metrics:inc('kafkahook.session_subscribed'),
    WorkerName = application:get_env(?APP, workername, undefined),
    Params = #{ type => subscribe
                , clientid => ClientId
                , username => Username
                , topic => Topic
                , workername => list_to_binary(WorkerName)
                , ts => erlang:system_time(millisecond)
                },
    produce_kafka_payload(ClientId, Params),
    ok.

on_session_unsubscribed(#{clientid := ClientId, username := Username}, Topic, _Opts, _Env) ->
    emqx_metrics:inc('kafkahook.session_unsubscribed'),
    WorkerName = application:get_env(?APP, workername, undefined),
    Params = #{ type => unsubscribe
                , clientid => ClientId
                , username => Username
                , topic => Topic
                , workername => list_to_binary(WorkerName)
                , ts => erlang:system_time(millisecond)
                },
    produce_kafka_payload(ClientId, Params),
    ok.


on_session_terminated(Info, {shutdown, Reason}, SessInfo, Env) when is_atom(Reason) ->
    on_session_terminated(Info, Reason, SessInfo, Env);
on_session_terminated(#{clientid := ClientId, username := Username}, Reason, _SessInfo, _Env) when is_atom(Reason) ->
    emqx_metrics:inc('kafkahook.session_terminated'),
    WorkerName = application:get_env(?APP, workername, undefined),
    Params = #{ type => session_terminated
              , clientid => ClientId
              , username => Username
              , reason => Reason
              , workername => list_to_binary(WorkerName)
              , ts => erlang:system_time(millisecond)
              },
    produce_kafka_payload(ClientId, Params),
    ok;
on_session_terminated(#{}, Reason, _SessInfo, _Env) ->
    ?LOG(error, "session.terminated can't encode the reason: ~p", [Reason]),
    ok.


%%--------------------------------------------------------------------
%% Message publish
%%--------------------------------------------------------------------

on_message_publish(Message = #message{topic = <<"$SYS/", _/binary>>}, _Env) ->
    {ok, Message};
on_message_publish(Message = #message{topic = Topic, flags = #{retain := Retain}}, _Env) ->
    emqx_metrics:inc('kafkahook.message_publish'),
    {FromClientId, FromUsername} = parse_from(Message),
    WorkerName = application:get_env(?APP, workername, undefined),
    Params = #{ type => publish
                , clientid => FromClientId
                , username => FromUsername
                , qos => Message#message.qos
                , msgid => binary:bin_to_list(Message#message.id)
                , pktid => 0
                , retain => Retain
                , topic => Message#message.topic
                , payload => binary:bin_to_list(Message#message.payload)
                , workername => list_to_binary(WorkerName)
                , ts => Message#message.timestamp
                },
    produce_kafka_payload(FromClientId, Params),
    {ok, Message}.

on_message_acked(#{clientid := _ClientId}, Message, _Env) ->
    emqx_metrics:inc('kafkahook.message_acked'),
    {FromClientId, FromUsername} = parse_from(Message),
    WorkerName = application:get_env(?APP, workername, undefined),
    Params = #{ type => acked
                , clientid => FromClientId
                , username => FromUsername
                , msgid => binary:bin_to_list(Message#message.id)
                , pktid => 0
                , topic => Message#message.topic
                , workername => list_to_binary(WorkerName)
                , ts => Message#message.timestamp
                },
    produce_kafka_payload(FromClientId, Params),
    ok.


brod_init() ->
    erlkaf:start(),

    ProducerConfig = [
        {bootstrap_servers, <<"kafka01.node.niu.local:9092">>},
        {delivery_report_only_error, false}
    ],
    ok = erlkaf:create_producer(client_producer, ProducerConfig).

produce_kafka_payload(Key, Message) ->
    Topic = application:get_env(?APP, topic, undefined),
    {ok, MessageJson} = emqx_json:safe_encode(Message),
    Payload = iolist_to_binary(MessageJson),
    emqx_metrics:inc('kafkahook.kafka_publish'),
    erlkaf:produce(client_producer, Topic, Key, Payload).

parse_from(#message{from = ClientId, headers = #{username := Username}}) ->
    {ClientId, maybe(Username)};
parse_from(#message{from = ClientId, headers = _HeadersNoUsername}) ->
    {ClientId, maybe(undefined)}.


stringfy(undefined) ->
    null;
stringfy(Term) when is_atom(Term); is_binary(Term) ->
    Term;
stringfy(Term) ->
    iolist_to_binary(io_lib:format("~0p", [Term])).

maybe(undefined) -> null;
maybe(Str) -> Str.