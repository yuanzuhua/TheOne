using System;
using System.Collections.Generic;
using System.Text;
using NUnit.Framework;
using TheOne.Redis.Client;
using TheOne.Redis.Common;

namespace TheOne.Redis.Tests.Examples {

    [TestFixture]
    internal sealed class UserSessionTests : RedisTestBase {

        #region Models

        public class CachedUserSessionManager {

            private readonly ICacheClient _cacheClient;

            public CachedUserSessionManager(ICacheClient cacheClient) {
                this._cacheClient = cacheClient;
            }

            /// <summary>
            ///     Removes the client session.
            /// </summary>
            /// <param name="userId" >The user global id.</param>
            /// <param name="clientSessionIds" >The client session ids.</param>
            public void RemoveClientSession(Guid userId, ICollection<Guid> clientSessionIds) {
                UserSession userSession = this.GetUserSession(userId);
                if (userSession == null) {
                    return;
                }

                foreach (Guid clientSessionId in clientSessionIds) {
                    userSession.RemoveClientSession(clientSessionId);
                }

                this.UpdateUserSession(userSession);
            }

            /// <summary>
            ///     Adds a new client session.
            ///     Should this be changed to GetOrCreateClientSession?
            /// </summary>
            /// <param name="userId" >The user global id.</param>
            /// <param name="userName" >Title of the user.</param>
            /// <param name="shardId" ></param>
            /// <param name="ipAddress" >The ip address.</param>
            /// <param name="base64ClientModulus" >The base64 client modulus.</param>
            /// <param name="userClientGlobalId" >The user client global id.</param>
            public UserClientSession StoreClientSession(Guid userId, string userName, string shardId, string ipAddress,
                string base64ClientModulus, Guid userClientGlobalId) {
                UserSession userSession = this.GetOrCreateSession(userId, userName, shardId);

                UserClientSession existingClientSession = userSession.GetClientSessionWithClientId(userClientGlobalId);
                if (existingClientSession != null) {
                    userSession.RemoveClientSession(existingClientSession.Id);
                }

                UserClientSession newClientSession = userSession.CreateNewClientSession(
                    ipAddress,
                    base64ClientModulus,
                    userClientGlobalId);

                this.UpdateUserSession(userSession);

                return newClientSession;
            }

            /// <summary>
            ///     Updates the UserSession in the cache, or removes expired ones.
            /// </summary>
            /// <param name="userSession" >The user session.</param>
            public void UpdateUserSession(UserSession userSession) {
                var hasSessionExpired = userSession.HasExpired();
                if (hasSessionExpired) {
                    Console.WriteLine("Session has expired, removing: " + userSession.ToCacheKey());
                    this._cacheClient.Remove(userSession.ToCacheKey());
                } else {
                    Console.WriteLine("Updating session: " + userSession.ToCacheKey());
                    this._cacheClient.Replace(userSession.ToCacheKey(), userSession, userSession.ExpiryDate.GetValueOrDefault());
                }
            }

            /// <summary>
            ///     Gets the user session if it exists or null.
            /// </summary>
            /// <param name="userId" >The user global id.</param>
            public UserSession GetUserSession(Guid userId) {
                var cacheKey = UserSession.ToCacheKey(userId);
                var bytes = this._cacheClient.Get<byte[]>(cacheKey);
                if (bytes != null) {
                    var modelStr = Encoding.UTF8.GetString(bytes);
                    Console.WriteLine("UserSession => " + modelStr);
                }

                return this._cacheClient.Get<UserSession>(cacheKey);
            }

            /// <summary>
            ///     Gets or create a user session if one doesn't exist.
            /// </summary>
            /// <param name="userId" >The user global id.</param>
            /// <param name="userName" >Title of the user.</param>
            /// <param name="shardId" >shardId</param>
            public UserSession GetOrCreateSession(Guid userId, string userName, string shardId) {
                UserSession userSession = this.GetUserSession(userId);
                if (userSession == null) {
                    userSession = new UserSession {
                        ShardId = shardId,
                        UserId = userId,
                        UserName = userName
                    };

                    this._cacheClient.Add(userSession.ToCacheKey(),
                        userSession,
                        userSession.ExpiryDate.GetValueOrDefault(DateTime.UtcNow) + TimeSpan.FromHours(1));
                }

                return userSession;
            }

            /// <summary>
            ///     Gets the user client session identified by the id if exists otherwise null.
            /// </summary>
            /// <param name="userId" >The user global id.</param>
            /// <param name="clientSessionId" >The client session id.</param>
            public UserClientSession GetUserClientSession(Guid userId, Guid clientSessionId) {
                UserSession userSession = this.GetUserSession(userId);
                return userSession?.GetClientSession(clientSessionId);
            }

        }

        public class UserClientSession : IHasRedisGuidId {

            private const int _validForTwoWeeks = 14;

            // Empty constructor required for TypeSerializer
            public UserClientSession() { }

            public UserClientSession(Guid sessionId, Guid userId, string ipAddress, string base64ClientModulus, Guid userClientGlobalId) {
                this.Id = sessionId;
                this.UserId = userId;
                this.IpAddress = ipAddress;
                this.Base64ClientModulus = base64ClientModulus;
                this.UserClientGlobalId = userClientGlobalId;
                this.ExpiryDate = DateTime.UtcNow.AddDays(_validForTwoWeeks);
            }

            // changed to public set
            public string IpAddress { get; set; }
            public DateTime ExpiryDate { get; set; }
            public Guid UserId { get; set; }
            public string Base64ClientModulus { get; set; }
            public Guid UserClientGlobalId { get; set; }

            public Guid Id { get; set; }

        }

        public class UserSession {

            // changed to public set
            public Guid UserId { get; set; }

            // changed to public set
            public string UserName { get; set; }

            // changed to public set
            public string ShardId { get; set; }

            // changed to public set
            public Dictionary<Guid, UserClientSession> PublicClientSessions { get; set; } = new Dictionary<Guid, UserClientSession>();

            /// <summary>
            ///     Gets the max expiry date of all the users client sessions.
            ///     If the user has no more active client sessions we can remove them from the cache.
            /// </summary>
            /// <value>The expiry date.</value>
            public DateTime? ExpiryDate {
                get {
                    DateTime? maxExpiryDate = null;

                    foreach (UserClientSession session in this.PublicClientSessions.Values) {
                        if (maxExpiryDate == null || session.ExpiryDate > maxExpiryDate) {
                            maxExpiryDate = session.ExpiryDate;
                        }
                    }

                    return maxExpiryDate;
                }
            }

            /// <summary>
            ///     Creates a new client session for the user.
            /// </summary>
            /// <param name="ipAddress" >The ip address.</param>
            /// <param name="base64ClientModulus" >The base64 client modulus.</param>
            /// <param name="userClientGlobalId" >The user client global id.</param>
            public UserClientSession CreateNewClientSession(string ipAddress, string base64ClientModulus, Guid userClientGlobalId) {
                return this.CreateClientSession(Guid.NewGuid(), ipAddress, base64ClientModulus, userClientGlobalId);
            }

            public UserClientSession CreateClientSession(Guid sessionId, string ipAddress, string base64ClientModulus,
                Guid userClientGlobalId) {
                var clientSession = new UserClientSession(
                    sessionId,
                    this.UserId,
                    ipAddress,
                    base64ClientModulus,
                    userClientGlobalId);

                this.PublicClientSessions[clientSession.Id] = clientSession;

                return clientSession;
            }

            /// <summary>
            ///     Removes the client session.
            /// </summary>
            /// <param name="clientSessionId" >The client session id.</param>
            public void RemoveClientSession(Guid clientSessionId) {
                if (this.PublicClientSessions.ContainsKey(clientSessionId)) {
                    this.PublicClientSessions.Remove(clientSessionId);
                }
            }

            public UserClientSession GetClientSessionWithClientId(Guid userClientId) {
                foreach (KeyValuePair<Guid, UserClientSession> entry in this.PublicClientSessions) {
                    if (entry.Value.UserClientGlobalId == userClientId) {
                        return entry.Value;
                    }
                }

                return null;
            }

            /// <summary>
            ///     Verifies this UserSession, removing any expired sessions.
            ///     Returns true to keep the UserSession in the cache.
            /// </summary>
            /// <returns>
            ///     <c>true</c> if this session has any active client sessions; otherwise, <c>false</c>.
            /// </returns>
            public bool HasExpired() {
                RemoveExpiredSessions(this.PublicClientSessions);

                // If there are no more active client sessions we can remove the entire UserSessions
                var sessionHasExpired =
                    this.ExpiryDate == null                      // There are no UserClientSessions
                    || this.ExpiryDate.Value <= DateTime.UtcNow; // The max UserClientSession ExpiryDate has expired

                return sessionHasExpired;
            }

            private static void RemoveExpiredSessions(IDictionary<Guid, UserClientSession> clientSessions) {
                var expiredSessionKeys = new List<Guid>();

                foreach (KeyValuePair<Guid, UserClientSession> clientSession in clientSessions) {
                    if (clientSession.Value.ExpiryDate < DateTime.UtcNow) {
                        expiredSessionKeys.Add(clientSession.Key);
                    }
                }

                foreach (Guid sessionKey in expiredSessionKeys) {
                    clientSessions.Remove(sessionKey);
                }
            }

            public void RemoveAllSessions() {
                this.PublicClientSessions.Clear();
            }

            public UserClientSession GetClientSession(Guid clientSessionId) {

                if (this.PublicClientSessions.TryGetValue(clientSessionId, out UserClientSession session)) {
                    return session;
                }

                return null;
            }

            public string ToCacheKey() {
                return ToCacheKey(this.UserId);
            }

            public static string ToCacheKey(Guid userId) {
                return UrnId.Create<UserSession>(userId.ToString());
            }

        }

        #endregion

        private const string _userName = "User1";
        private const string _shardId = "0";

        private static readonly Guid _userClientGlobalId1 = new Guid("71A30DE3-D7AF-4B8E-BCA2-AB646EE1F3E9");
        private static readonly Guid _userClientGlobalId2 = new Guid("A8D300CF-0414-4C99-A495-A7F34C93CDE1");
        private static readonly string _userClientKey = new Guid("10B7D0F7-4D4E-4676-AAC7-CF0234E9133E").ToString("N");
        private static readonly Guid _userId = new Guid("5697B030-A369-43A2-A842-27303A0A62BC");

        private readonly UserClientSession _session = new UserClientSession(
            Guid.NewGuid(),
            _userId,
            Config.MasterHost,
            _userClientKey,
            _userClientGlobalId1);

        private RedisClient _redisCache;

        public CachedUserSessionManager GetCacheManager(ICacheClient cacheClient) {
            return new CachedUserSessionManager(cacheClient);
        }

        private static void AssertClientSessionsAreEqual(
            UserClientSession clientSession, UserClientSession resolvedClientSession) {
            Assert.That(resolvedClientSession.Id, Is.EqualTo(clientSession.Id));
            Assert.That(resolvedClientSession.Base64ClientModulus, Is.EqualTo(clientSession.Base64ClientModulus));
            Assert.That(resolvedClientSession.IpAddress, Is.EqualTo(clientSession.IpAddress));
            Assert.That(resolvedClientSession.UserClientGlobalId, Is.EqualTo(clientSession.UserClientGlobalId));
            Assert.That(resolvedClientSession.UserId, Is.EqualTo(clientSession.UserId));
        }

        [SetUp]
        public void OnBeforeEachTest() {
            this._redisCache = new RedisClient(Config.MasterHost);
            this._redisCache.FlushAll();
        }

        [Test]
        public void Can_add_multiple_UserClientSessions() {
            CachedUserSessionManager cacheManager = this.GetCacheManager(this._redisCache);

            UserClientSession clientSession1 = cacheManager.StoreClientSession(
                _userId,
                _userName,
                _shardId,
                this._session.IpAddress,
                _userClientKey,
                _userClientGlobalId1);

            UserClientSession clientSession2 = cacheManager.StoreClientSession(
                _userId,
                _userName,
                _shardId,
                this._session.IpAddress,
                _userClientKey,
                _userClientGlobalId2);

            UserClientSession resolvedClientSession1 = cacheManager.GetUserClientSession(
                clientSession1.UserId,
                clientSession1.Id);

            UserClientSession resolvedClientSession2 = cacheManager.GetUserClientSession(
                clientSession2.UserId,
                clientSession2.Id);

            AssertClientSessionsAreEqual(clientSession1, resolvedClientSession1);
            AssertClientSessionsAreEqual(clientSession2, resolvedClientSession2);
        }

        [Test]
        public void Can_add_single_UserSession() {
            CachedUserSessionManager cacheManager = this.GetCacheManager(this._redisCache);

            UserClientSession clientSession = cacheManager.StoreClientSession(
                _userId,
                _userName,
                _shardId,
                this._session.IpAddress,
                _userClientKey,
                _userClientGlobalId1);

            UserClientSession resolvedClientSession = cacheManager.GetUserClientSession(clientSession.UserId, clientSession.Id);

            AssertClientSessionsAreEqual(clientSession, resolvedClientSession);
        }

        [Test]
        public void Does_remove_UserClientSession() {
            CachedUserSessionManager cacheManager = this.GetCacheManager(this._redisCache);

            UserClientSession clientSession1 = cacheManager.StoreClientSession(
                _userId,
                _userName,
                _shardId,
                this._session.IpAddress,
                _userClientKey,
                _userClientGlobalId1);

            UserSession userSession = cacheManager.GetUserSession(_userId);
            UserClientSession resolvedClientSession1 = userSession.GetClientSession(clientSession1.Id);
            AssertClientSessionsAreEqual(resolvedClientSession1, clientSession1);

            resolvedClientSession1.ExpiryDate = DateTime.UtcNow.AddSeconds(-1);
            cacheManager.UpdateUserSession(userSession);

            userSession = cacheManager.GetUserSession(_userId);
            Assert.That(userSession, Is.Null);
        }

    }

}
