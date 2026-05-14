defmodule Tailscale.Test do
  use ExUnit.Case, async: true

  import Tailscale.Test.Helpers,
    only: [check_net: 1, auth_key: 1, state_file: 1, connected_client: 1]

  @net_skip !Tailscale.Test.Helpers.enable_net_tests()

  describe "client connect" do
    setup [:check_net, :auth_key, :state_file]

    @tag skip: @net_skip
    test "connect", %{state_file: state_file, auth_key: auth_key} do
      {:ok, dev} = Tailscale.connect(state_file, auth_key: auth_key)
      IO.puts("connected!")

      {:ok, ip} = Tailscale.ipv4_addr(dev)
      IO.puts("tailnet ip: #{ip |> :inet.ntoa()}")
    end
  end

  describe "connected client" do
    setup [:connected_client]

    @tag skip: @net_skip
    test "ip4", %{ipv4: ip} do
      assert :inet.is_ipv4_address(ip)
    end

    @tag skip: @net_skip
    test "ip6", %{ipv6: ip} do
      assert :inet.is_ipv6_address(ip)
    end

    @tag skip: @net_skip
    test "udp bind", %{ts: dev, ipv4: ip} do
      {:ok, _sock} = Tailscale.Udp.bind(dev, ip, 1234)
    end

    @tag skip: @net_skip
    test "tcp listen", %{ts: dev, ipv4: ip} do
      {:ok, _sock} = Tailscale.Tcp.listen(dev, ip, 1234)
    end
  end

  describe "tcp recv" do
    setup [:connected_client]

    defp tcp_pair(dev, ip, port) do
      {:ok, listener} = Tailscale.Tcp.listen(dev, ip, port)

      accept_task = Task.async(fn -> Tailscale.Tcp.Listener.accept(listener) end)
      {:ok, client} = Tailscale.Tcp.connect(dev, ip, port)
      {:ok, server} = Task.await(accept_task)

      {client, server}
    end

    @tag skip: @net_skip
    test "recv/2 returns at most max_bytes", %{ts: dev, ipv4: ip} do
      {client, server} = tcp_pair(dev, ip, 2001)

      :ok = Tailscale.Tcp.Stream.send_all(client, "hello world")
      {:ok, data} = Tailscale.Tcp.Stream.recv(server, 5)

      assert byte_size(data) <= 5
    end

    @tag skip: @net_skip
    test "recv/2 with max_bytes 0 returns available data", %{ts: dev, ipv4: ip} do
      {client, server} = tcp_pair(dev, ip, 2002)

      :ok = Tailscale.Tcp.Stream.send_all(client, "hello")
      {:ok, data} = Tailscale.Tcp.Stream.recv(server, 0)

      assert data == "hello"
    end

    @tag skip: @net_skip
    test "recv/3 with timeout returns data when available", %{ts: dev, ipv4: ip} do
      {client, server} = tcp_pair(dev, ip, 2003)

      :ok = Tailscale.Tcp.Stream.send_all(client, "hi")
      {:ok, data} = Tailscale.Tcp.Stream.recv(server, 10, 5000)

      assert data == "hi"
    end

    @tag skip: @net_skip
    test "recv/3 returns {:error, :timeout} when no data arrives", %{ts: dev, ipv4: ip} do
      {_client, server} = tcp_pair(dev, ip, 2004)

      assert {:error, :timeout} = Tailscale.Tcp.Stream.recv(server, 10, 100)
    end

    @tag skip: @net_skip
    test "recv/3 with :infinity timeout blocks until data", %{ts: dev, ipv4: ip} do
      {client, server} = tcp_pair(dev, ip, 2005)

      Task.async(fn ->
        Process.sleep(100)
        Tailscale.Tcp.Stream.send_all(client, "delayed")
      end)

      {:ok, data} = Tailscale.Tcp.Stream.recv(server, 100, :infinity)
      assert data == "delayed"
    end

    @tag skip: @net_skip
    test "recv/2 multiple reads consume stream incrementally", %{ts: dev, ipv4: ip} do
      {client, server} = tcp_pair(dev, ip, 2006)

      :ok = Tailscale.Tcp.Stream.send_all(client, "abcdefghij")

      # Read in small chunks — we may get fewer bytes than max_bytes per read,
      # but accumulating should yield the full payload.
      {:ok, chunk1} = Tailscale.Tcp.Stream.recv(server, 4)
      assert byte_size(chunk1) <= 4 and byte_size(chunk1) > 0

      # Keep reading until we have all 10 bytes
      remaining = 10 - byte_size(chunk1)
      {:ok, chunk2} = Tailscale.Tcp.Stream.recv(server, remaining)

      assert chunk1 <> chunk2 == "abcdefghij"
    end
  end
end
