-module(codex_pooler_dependency_smtp_server).
-behaviour(gen_smtp_server_session).

-export([
    init/4,
    'handle_HELO'/2,
    'handle_EHLO'/3,
    'handle_STARTTLS'/1,
    'handle_MAIL'/2,
    'handle_RCPT'/2,
    'handle_DATA'/4,
    'handle_MAIL_extension'/2,
    'handle_RCPT_extension'/2,
    'handle_RSET'/1,
    'handle_VRFY'/2,
    'handle_AUTH'/4,
    handle_other/3,
    handle_error/3,
    handle_info/2,
    terminate/2,
    code_change/3
]).

init(_Hostname, _Count, _Peer, Options) ->
    Owner = proplists:get_value(owner, Options),
    Owner ! {smtp_session, self()},
    {ok, "example.com ESMTP", #{owner => Owner, tls => false}}.

'handle_HELO'(_Hostname, State) -> {ok, State}.
'handle_EHLO'(_Hostname, Extensions, State) ->
    {ok, Extensions ++ [{"STARTTLS", true}], State}.
'handle_STARTTLS'(#{owner := Owner} = State) ->
    Owner ! {smtp_tls, self()},
    State#{tls := true}.
'handle_MAIL'(_From, State) -> {ok, State}.
'handle_RCPT'(_To, State) -> {ok, State}.
'handle_DATA'(_From, Recipients, Data, #{owner := Owner, tls := TLS} = State) ->
    Owner ! {smtp_delivered, TLS, length(Recipients), byte_size(Data)},
    {ok, "accepted", State}.
'handle_MAIL_extension'(_Extension, _State) -> error.
'handle_RCPT_extension'(_Extension, _State) -> error.
'handle_RSET'(State) -> State.
'handle_VRFY'(_Address, State) -> {error, "252 unavailable", State}.
'handle_AUTH'(_Type, _Username, _Credential, _State) -> error.
handle_other(_Verb, _Args, State) -> {"500 unsupported", State}.
handle_error(_Error, _Message, State) -> {ok, State}.
handle_info(_Message, State) -> {noreply, State}.
terminate(_Reason, _State) -> ok.

code_change(_Old, State, same) -> {ok, State};
code_change(_Old, State, throw_same) -> throw({ok, State});
code_change(_Old, _State, throw) -> throw(synthetic_throw);
code_change(_Old, _State, exit) -> exit(synthetic_exit);
code_change(_Old, _State, error) -> erlang:error(synthetic_error);
code_change(_Old, _State, other) -> other;
code_change(_Old, State, changed) -> {ok, State#{changed => true}}.
