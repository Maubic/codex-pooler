defmodule CodexPooler.Gateway.RequestCompression.JsonMinifierTest do
  use ExUnit.Case, async: true

  alias CodexPooler.Gateway.RequestCompression.JsonMinifier

  test "removes only whitespace between tokens and keeps every token byte" do
    original = ~s( \n{ "a b" : [ 1.50 , -0 , 1E+2 ,\t"x\\" y" , "\\\\" , "\\u00e9\\/" ] ,\r\n "a b" : null }\n )

    assert JsonMinifier.minify(original) == ~S({"a b":[1.50,-0,1E+2,"x\" y","\\","\u00e9\/"],"a b":null})
  end

  test "keeps whitespace inside strings, including after escaped backslashes" do
    original = ~s({"text": "keep  two\\\\ spaces  ", "next":  "\\\\"  })

    assert JsonMinifier.minify(original) == ~S({"text":"keep  two\\ spaces  ","next":"\\"})
  end

  test "leaves already-minified documents unchanged and is idempotent" do
    compact = ~S({"rows":[{"id":1,"v":"a b"},{"id":2,"v":[]}],"n":0.1})
    pretty = CodexPooler.JSON.encode!(CodexPooler.JSON.decode!(compact, objects: :ordered_objects), pretty: true)

    assert JsonMinifier.minify(compact) == compact
    assert JsonMinifier.minify(pretty) == compact
    assert pretty |> JsonMinifier.minify() |> JsonMinifier.minify() == compact
  end
end
