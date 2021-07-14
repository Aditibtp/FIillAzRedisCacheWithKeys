using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using StackExchange.Redis;
using System.Diagnostics;
using System.Security.Authentication;
using System.Net;
using System.IO;
using System.Net.Security;
using System.Net.Sockets;
using Microsoft.Azure.Services.AppAuthentication;
using Microsoft.Azure.KeyVault;

namespace RedisCacheConsoleApp
{
    class Program
    {
        private static Lazy<ConnectionMultiplexer> lazyConnection = new Lazy<ConnectionMultiplexer>(() => GetNewMux().Result);

        private static async Task<ConnectionMultiplexer> GetNewMux()
        {
            //string kvUri = Environment.GetEnvironmentVariable("KVURL");
            string kvUri = "<Key Vault url>";
            //string cacheKey = "keyr2";
            //string cacheUrl = "rcacheurl2";
            string cacheKey = "<cache key>";
            string cacheUrl = "cache URL";
            var tokenProvider = new AzureServiceTokenProvider();
            var keyVaultClient = new KeyVaultClient(new KeyVaultClient.AuthenticationCallback(tokenProvider.KeyVaultTokenCallback));

            var pass = await keyVaultClient.GetSecretAsync(kvUri, cacheKey);
            var url = await keyVaultClient.GetSecretAsync(kvUri, cacheUrl);
            string connectionStr = url.Value + " ,password=" + pass.Value;

            return ConnectionMultiplexer.Connect(connectionStr);
        }

        public static ConnectionMultiplexer Connection
        {
            get
            {
                return lazyConnection.Value;
            }
        }
        public static IDatabase cache;
        static void Main(string[] args)
        {
            ThreadPool.SetMinThreads(200, 200);
            //connection = Connection.GetDatabase();
            //var options = ConfigurationOptions.Parse(connectionStr);
            //options.SyncTimeout = 100000;
            Console.WriteLine("Connected : " + Connection.IsConnected);
            IDatabase db = Connection.GetDatabase(0);
            Console.WriteLine("Total keys in cache : " + db.Execute("DBSIZE").ToString());
            //Console.WriteLine("Total keys in cache : " + db.Execute("INFO").ToString());

            long keys = 0;
            for(int i=0;i <10; i++)
            {
               keys += AddDataBulk(Connection, numKeysPerShard: 200000, valueSize: 2000);
               Console.WriteLine("Added {0}", keys);
            }

            Console.WriteLine("All Done! Keys added: " + keys);
            Console.ReadLine();
        }

        /// <summary>
        /// Adds data to a redis cache using Lua scripting. Ensure that timeout settings are set appropriately on the
        /// ConnectionMultiplexer as each script evaluation may take a few seconds.
        /// </summary>
        /// <returns>The number of keys added to the cache</returns>
        public static long AddDataBulk(ConnectionMultiplexer redis, long numKeysPerShard, int valueSize)
        {
            IDatabase db = redis.GetDatabase(0);

            // total number of keys set, which is returned at the end of this method
            long totalKeys = 0;

            // generate a random value to use for all keys
            var rand = new Random();
            byte[] value = new byte[valueSize];
            rand.NextBytes(value);

            // in order to quickly populate a cache with data, we execute a small number of Lua scripts on each shard
            var clusterConfig = redis.GetServer(redis.GetEndPoints().First()).ClusterConfiguration;
            
            //Console.WriteLine($"cluster config: {clusterConfig.Nodes.Where(n => !n.IsReplica).Count()}");

            // non clustered case
            if (clusterConfig == null)
            {
                AddDataWithLuaScript(redis, numKeysPerShard, value, rand.NextDouble().ToString());
                return numKeysPerShard;
            }

            

            // clustered case
            foreach (var shard in clusterConfig.Nodes.Where(n => !n.IsReplica))
            {
                long totalKeysThisShard = 0;
                // numSlots is the number of slots over which to distribute a shard's keys
                int numSlots = Math.Min(100, shard.Slots.Sum((range) => range.To - range.From));
                var slots = new List<int>();
                for (int i = 0; i < numSlots; i++)
                {
                    // compute a value that hashes to this node and use it as a hash tag
                    var hashTagAndSlot = GetHashTagForNode(redis, shard.Slots, slots, rand);
                    // if numKeysPerShard < numSlots, put 1 key into the first numKeysPerShard slots
                    long numKeys = Math.Max(numKeysPerShard / numSlots, totalKeysThisShard < numKeysPerShard ? 1 : 0);

                    //AddDataWithLuaScript(redis, numKeys, value, hashTagAndSlot.Item1);
                    totalKeysThisShard += numKeys;
                    slots.Add(hashTagAndSlot.Item2);
                }
                totalKeys += totalKeysThisShard;
            }
            
            return totalKeys;
        }

        private static Tuple<string, int> GetHashTagForNode(ConnectionMultiplexer mx, IList<SlotRange> slotRanges, IList<int> excludedSlots, Random rand)
        {
            string hashTag = null;
            while (true)
            {
                hashTag = "-{" + rand.Next() + "}-";
                int slot = mx.HashSlot(hashTag);
                if ((excludedSlots == null || !excludedSlots.Contains(slot)) &&
                    (slotRanges.Any((range) => range.From <= slot && slot <= range.To)))
                {
                    return new Tuple<string, int>(hashTag, slot);
                }
            }
        }

        private static void AddDataWithLuaScript(ConnectionMultiplexer mx, long numKeys, byte[] value, string hashTag)
        {
            string script = @"
                        local i = 0
                        while i < tonumber(@numKeys) do
                            redis.call('set', 'key'..@hashTag..i, @value)
                            i = i + 1
                        end
                    ";

            var prepared = LuaScript.Prepare(script);
            mx.GetDatabase(0).ScriptEvaluate(prepared, new { numKeys = numKeys, value = value, hashTag = (RedisKey)hashTag });
        }
    }
}
