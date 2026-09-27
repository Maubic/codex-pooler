defmodule CodexPoolerWeb.Admin.RequestLogsUserAgentsTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Phoenix.LiveViewTest, only: [render_component: 2]

  alias CodexPoolerWeb.Admin.RequestLogsDisplay.UserAgents
  alias CodexPoolerWeb.Admin.RequestLogsPresentation

  describe "classify/1" do
    test "recognizes observed production and local request user agents" do
      assert %{kind: "codex_desktop", label: "Codex Desktop"} =
               UserAgents.classify("Codex Desktop/0.133.0-alpha.1 (Mac OS 26.5.0; arm64) unknown (Codex Desktop; 26.519.41501)")

      assert %{kind: "codex", label: "Codex", icon_class: ["size-3.5 shrink-0", "text-info"]} =
               UserAgents.classify("codex-tui/0.133.0 (Mac OS 26.5.0; arm64)")

      assert %{kind: "codex", label: "Codex"} =
               UserAgents.classify("codex_exec/0.133.0 (Alpine Linux 3.23.3; aarch64)")

      assert %{kind: "openai_python", label: "OpenAI Python SDK"} =
               UserAgents.classify("OpenAI/Python 2.38.0")

      assert %{kind: "openai_python", label: "OpenAI Python SDK"} =
               UserAgents.classify("AsyncOpenAI/Python 2.36.0")

      assert %{kind: "openai_node", label: "OpenAI Node SDK"} =
               UserAgents.classify("OpenAI/JS 6.39.0")

      assert %{kind: "vercel_ai_sdk", label: "Vercel AI SDK"} =
               UserAgents.classify("ai/6.0.191 ai-sdk/provider-utils/4.0.27 runtime/node.js/26")

      assert %{kind: "vercel_ai_sdk", label: "Vercel AI SDK"} =
               UserAgents.classify("ai/7.0.0-beta.12 @ai-sdk/openai/4.0.0-beta.12 runtime/node.js/24")

      assert %{kind: "python", label: "Python"} = UserAgents.classify("python-requests/2.33.0")
      assert %{kind: "python", label: "Python"} = UserAgents.classify("Python-urllib/3.14")
      assert %{kind: "node", label: "Node.js"} = UserAgents.classify("node")
      assert %{kind: "curl", label: "curl"} = UserAgents.classify("curl/8.20.0")
      assert %{kind: "elixir_http", label: "Elixir HTTP"} = UserAgents.classify("req/0.5.17")
      assert %{kind: "elixir_http", label: "Elixir HTTP"} = UserAgents.classify("mint/1.8.0")
    end

    test "recognizes documented harness names when they appear in user agents" do
      assert %{kind: "opencode", label: "opencode"} = UserAgents.classify("opencode/0.15.0")
      assert %{kind: "openclaw", label: "OpenClaw"} = UserAgents.classify("OpenClaw/1.0")
      assert %{kind: "hermes", label: "Hermes Agent"} = UserAgents.classify("Hermes-Agent/0.9")
      assert %{kind: "windmill", label: "Windmill"} = UserAgents.classify("windmill/beta")
      assert %{kind: "aider", label: "Aider"} = UserAgents.classify("aider/0.86.2")
      assert %{kind: "continue", label: "Continue"} = UserAgents.classify("Continue/1.5.45")
      assert %{kind: "cline", label: "Cline"} = UserAgents.classify("Cline/3.16.0")
      assert %{kind: "goose", label: "Goose"} = UserAgents.classify("goose/1.35.0")
    end

    test "classifies the observed DeepSeek Harness user agent" do
      user_agent = "deepseek-harness/0.1.5-rc.3 (+https://github.com/deepseek-ai/deepseek-harness)"

      assert %{kind: "deepseek_harness", label: "DeepSeek Harness"} = UserAgents.classify(user_agent)

      assert %{kind: "deepseek_harness", label: "DeepSeek Harness", text: "deepseek-harness 0.1.5-rc.3"} =
               UserAgents.display(%{user_agent: user_agent})

      assert %{kind: "unknown", label: "Client"} = UserAgents.classify("unrelated-client/1.0")
    end

    test "selects local logos for explicit client prefixes" do
      for {user_agent, kind, asset} <- [
            {"litellm/1.2.3", "litellm", "litellm-32.png"},
            {"omp/1.2.3", "omp", "omp.svg"},
            {"pi/1.2.3", "pi", "pi.svg"},
            {"Cursor/1.2.3", "cursor", "cursor.svg"},
            {"Kilo-Code/1.2.3 ai-sdk/provider-utils/1.2.3", "kilo", "kilocode.svg"},
            {"OpenHands/1.2.3", "openhands", "openhands.svg"},
            {"Trae/1.2.3", "trae", "trae.svg"},
            {"codex_vscode/1.2.3", "codex", "codex.svg"},
            {"codex_cli_rs/1.2.3", "codex", "codex.svg"},
            {"codex-chrome-extension-sidepanel/1.2.3", "codex", "codex.svg"},
            {"hermes-cli/1.2.3", "hermes", "hermesagent.svg"}
          ] do
        assert %{kind: ^kind, logo: %{asset: ^asset}} = UserAgents.classify(user_agent)
        assert File.regular?(Application.app_dir(:codex_pooler, "priv/static/images/client-logos/#{asset}"))
      end

      assert %{kind: "openai_python", logo: %{asset: "openai.svg"}} = UserAgents.classify("OpenAI/Python 1.2.3")
      assert %{kind: "node", logo: %{asset: "nodedotjs.svg"}} = UserAgents.classify("node")
      assert %{kind: "unknown", logo: nil} = UserAgents.classify("my-litellm-wrapper/1.2.3")
      assert %{kind: "unknown", logo: nil} = UserAgents.classify("cursor-probe/1.2.3")
    end
  end

  test "renders the selected vector mask or raster asset with decorative semantics" do
    vector =
      render_component(&RequestLogsPresentation.request_log_user_agent_icon/1,
        user_agent: UserAgents.display(%{user_agent: "opencode/1.2.3"})
      )
      |> LazyHTML.from_fragment()

    assert Enum.count(LazyHTML.query(vector, "[data-role='user-agent-icon'][aria-hidden='true'] [data-role='user-agent-logo'][data-logo='opencode.svg']")) == 1
    assert LazyHTML.query(vector, "[data-role='user-agent-logo']") |> LazyHTML.attribute("style") == ["mask-image: url(/images/client-logos/opencode.svg)"]

    raster =
      render_component(&RequestLogsPresentation.request_log_user_agent_icon/1,
        user_agent: UserAgents.display(%{user_agent: "litellm/1.2.3"})
      )
      |> LazyHTML.from_fragment()

    assert Enum.count(LazyHTML.query(raster, "[aria-hidden='true'] img[src='/images/client-logos/litellm-32.png'][alt=''][width='14'][height='14']")) == 1
  end

  test "unknown and absent user agents retain fallback and omission behavior" do
    unknown = UserAgents.display(%{user_agent: "sample-client/1.2.3"})
    assert %{logo: nil, text: "sample-client 1.2.3"} = unknown

    document =
      render_component(&RequestLogsPresentation.request_log_user_agent_icon/1, user_agent: unknown)
      |> LazyHTML.from_fragment()

    assert Enum.count(LazyHTML.query(document, "[data-role='user-agent-icon'] .hero-window")) == 1
    assert Enum.empty?(LazyHTML.query(document, "[data-role='user-agent-logo']"))
    assert is_nil(UserAgents.display(%{user_agent: nil}))
    assert is_nil(UserAgents.display(%{user_agent: " "}))
  end

  test "display/1 keeps compact sanitized text separate from classification" do
    assert %{
             kind: "codex_desktop",
             label: "Codex Desktop",
             title: "Codex Desktop user agent",
             text: "Codex Desktop 0.133.0-alpha.1"
           } =
             UserAgents.display(%{
               user_agent: "Codex Desktop/0.133.0-alpha.1 (Mac OS 26.5.0; arm64) unknown (Codex Desktop; 26.519.41501)"
             })
  end
end
