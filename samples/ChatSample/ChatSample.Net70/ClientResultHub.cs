using System;
using System.Buffers;
using System.IdentityModel.Tokens.Jwt;
using System.Net.Http.Headers;
using System.Text;
using MessagePack;
using Microsoft.AspNetCore.SignalR;
using Microsoft.IdentityModel.Tokens;

namespace ClientResultSample
{
    public class ClientResultHub : Hub
    {
        public override Task OnConnectedAsync()
        {
            _ = StartServerStream(Context.ConnectionId);
            return Clients.All.SendAsync("Connect", $"Connection '{Context.ConnectionId}' is connected.");
        }

        private async Task StartServerStream(string connectionId)
        {
            await Task.Delay(1000);
            var invocationId = Guid.NewGuid().ToString();
            using HttpClient httpClient = new();
            var url = "https://<endpoint>/api/hubs/ClientResultHub/:send";
            var json = $"{{\"invocationId\":\"{invocationId}\",\"target\":\"StreamBroadcast\",\"type\":4}}\x1e";
            await Send(httpClient, url, json);
            await Task.Delay(100);
            json = $"{{\"invocationId\":\"{invocationId}\",\"item\":\"Welcome \",\"type\":2}}\x1e";
            await Send(httpClient, url, json);
            for (int i = 0; i < connectionId.Length; i++)
            {
                json = $"{{\"invocationId\":\"{invocationId}\",\"item\":\"{connectionId[i]}\",\"type\":2}}\x1e";
                await Send(httpClient, url, json);
                await Task.Delay(100);
            }
            json = $"{{\"invocationId\":\"{invocationId}\",\"type\":3}}\x1e";
            await Send(httpClient, url, json);
        }

        private static async Task Send(HttpClient httpClient, string url, string json)
        {
            using (var resp = await httpClient.SendAsync(CreateRequest(url, Encoding.UTF8.GetBytes(json)), default))
            {
                Console.WriteLine(resp.StatusCode);
            }
        }

        private static HttpRequestMessage CreateRequest(string url, byte[] content)
        {
            var req = new HttpRequestMessage(HttpMethod.Post, url);
            req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", GetAccessToken(url, "<Key>", "123"));
            Console.WriteLine(req.Headers.Authorization);
            var ms = new ArrayBufferWriter<byte>();
            MessagePackWriter writer = new(ms);
            writer.WriteMapHeader(1);
            writer.Write("json");
            writer.Write(content);
            req.Content = new ReadOnlyMemoryContent(ms.GetMemory());
            req.Content.Headers.ContentType = MediaTypeHeaderValue.Parse("application/octet-stream");
            return req;
        }

        private static string GetAccessToken(string url, string signingKey, string? kid = null)
        {
            var securityKey = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(signingKey)) { KeyId = kid ?? signingKey.GetHashCode().ToString() };
            var credentials = new SigningCredentials(securityKey, SecurityAlgorithms.HmacSha256);

            JwtSecurityTokenHandler JwtTokenHandler = new();
            var token = JwtTokenHandler.CreateJwtSecurityToken(
                subject: null,
                audience: url,
                expires: DateTime.UtcNow.AddHours(1),
                signingCredentials: credentials);
            return JwtTokenHandler.WriteToken(token);
        }

        public async Task<string> GetMessage(string ID)
        {
            try
            {
                var res = await Clients.Client(ID).InvokeAsync<string>("GetMessage", default);
                return $"From {ID}: {res}";
            }
            catch (Exception ex)
            {
                return $"[Error] Failed invoke connection {ID}]: {ex.Message}";
            }
        }

        public async Task Broadcast(string message)
        {
            await Clients.All.SendAsync("Broadcast", $"Broadcast from '{Context.ConnectionId}': {message}");
        }

        public override Task OnDisconnectedAsync(Exception? exception)
        {
            return Clients.All.SendAsync("Connect", $"Connection '{Context.ConnectionId}' is disconnected.");
        }
    }
}
