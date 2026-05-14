defmodule Tailscale.Tcp.Stream do
  @moduledoc """
  Tailscale TCP sockets (connected).
  """

  @typedoc """
  A handle to a TCP stream (connected socket).
  """
  @opaque t :: Tailscale.Native.tcp_stream()

  @spec send(t(), binary()) :: {:ok, integer()} | {:error, any()}
  @doc """
  Send data over the TCP socket, blocking until at least one byte can be sent.

  Returns the number of bytes actually sent.
  """
  def send(res, msg) do
    Tailscale.Native.tcp_send(res, msg)
  end

  @spec send_all(t(), binary()) :: :ok | {:error, any()}
  @doc """
  Send the payload over the TCP socket, blocking until it is fully sent.
  """
  def send_all(res, msg) do
    len = byte_size(msg)

    case Tailscale.Tcp.Stream.send(res, msg) do
      {:ok, ^len} -> :ok
      {:ok, n} -> Tailscale.Tcp.Stream.send_all(res, binary_slice(msg, n..len))
      err -> err
    end
  end

  @spec recv(t()) :: {:ok, binary()} | {:error, any()}
  @doc """
  Receive data from the TCP socket, blocking until at least one byte can be received.
  """
  def recv(res) do
    Tailscale.Native.tcp_recv(res)
  end

  @spec recv(t(), non_neg_integer()) :: {:ok, binary()} | {:error, any()}
  @doc """
  Receive up to `max_bytes` from the TCP socket, blocking until at least one byte can be received.

  Returns at most `max_bytes` bytes. If `max_bytes` is 0, returns whatever data is available
  (equivalent to `recv/1`).
  """
  def recv(res, max_bytes) do
    recv(res, max_bytes, :infinity)
  end

  @spec recv(t(), non_neg_integer(), timeout()) :: {:ok, binary()} | {:error, any()}
  @doc """
  Receive up to `max_bytes` from the TCP socket, with a timeout.

  - `max_bytes`: maximum number of bytes to return. 0 means return whatever is available.
  - `timeout`: timeout in milliseconds, or `:infinity` for no timeout.

  Returns `{:error, :timeout}` if the timeout expires before data arrives.
  """
  def recv(res, max_bytes, :infinity) do
    Tailscale.Native.tcp_recv_max(res, max_bytes, 0)
  end

  def recv(res, max_bytes, timeout) when is_integer(timeout) and timeout >= 0 do
    case Tailscale.Native.tcp_recv_max(res, max_bytes, timeout) do
      {:error, "timeout"} -> {:error, :timeout}
      other -> other
    end
  end

  @spec local_addr(t()) :: {:inet.ip_address(), :inet.port_number()}
  @doc """
  Get the local address on which this TCP stream is bound.
  """
  def local_addr(stream) do
    Tailscale.Native.tcp_local_addr(stream)
  end

  @spec remote_addr(t()) :: {:inet.ip_address(), :inet.port_number()}
  @doc """
  Get the remote address to which this TCP stream is connected.
  """
  def remote_addr(stream) do
    Tailscale.Native.tcp_remote_addr(stream)
  end
end
