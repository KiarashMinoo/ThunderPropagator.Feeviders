using System.Security.Cryptography;
using System.Text;

namespace ThunderPropagator.Feeviders.UdpClient.SharedKernel
{
    internal sealed class UdpMessageProtector : IDisposable
    {
        private const byte ProtocolVersion = 1;
        private const int SaltSize = 16;
        private const int InitializationVectorSize = 16;
        private const int AuthenticationTagSize = 32;
        private const int EncryptionKeySize = 32;
        private const int AuthenticationKeySize = 32;
        private const int Pbkdf2IterationCount = 600_000;
        private const int AuthenticatedHeaderSize = 1 + SaltSize + InitializationVectorSize;
        private const int EncryptedPayloadOffset = AuthenticatedHeaderSize + AuthenticationTagSize;
        private const int MinimumEncryptedMessageSize = EncryptedPayloadOffset + InitializationVectorSize;

        private readonly byte[] _secret;
        private byte[]? _encryptionSalt;
        private byte[]? _encryptionKey;
        private byte[]? _encryptionAuthenticationKey;
        private byte[]? _decryptionSalt;
        private byte[]? _decryptionKey;
        private byte[]? _decryptionAuthenticationKey;
        private bool _disposed;

        public UdpMessageProtector(string secret)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(secret);
            _secret = Encoding.UTF8.GetBytes(secret);
        }

        public byte[] Protect(byte[] plainData)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            ArgumentNullException.ThrowIfNull(plainData);
            EnsureEncryptionKeys();

            using var aes = CreateAes();
            aes.GenerateIV();
            using var encryptor = aes.CreateEncryptor(_encryptionKey!, aes.IV);
            var encryptedPayload = encryptor.TransformFinalBlock(plainData, 0, plainData.Length);

            var result = new byte[EncryptedPayloadOffset + encryptedPayload.Length];
            result[0] = ProtocolVersion;
            _encryptionSalt!.CopyTo(result.AsSpan(1, SaltSize));
            aes.IV.CopyTo(result.AsSpan(1 + SaltSize, InitializationVectorSize));
            encryptedPayload.CopyTo(result.AsSpan(EncryptedPayloadOffset));

            var authenticationTag = ComputeAuthenticationTag(
                _encryptionAuthenticationKey!,
                result.AsSpan(0, AuthenticatedHeaderSize),
                result.AsSpan(EncryptedPayloadOffset));
            authenticationTag.CopyTo(result.AsSpan(AuthenticatedHeaderSize, AuthenticationTagSize));
            CryptographicOperations.ZeroMemory(authenticationTag);

            return result;
        }

        public byte[] Unprotect(byte[] encryptedMessage)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            ArgumentNullException.ThrowIfNull(encryptedMessage);

            if (encryptedMessage.Length < MinimumEncryptedMessageSize)
                throw new CryptographicException("Encrypted UDP message is too short.");

            var message = encryptedMessage.AsSpan();
            if (message[0] != ProtocolVersion)
                throw new CryptographicException("Encrypted UDP message version is not supported.");

            var salt = message.Slice(1, SaltSize);
            EnsureDecryptionKeys(salt);

            var receivedAuthenticationTag = message.Slice(AuthenticatedHeaderSize, AuthenticationTagSize);
            var encryptedPayload = message.Slice(EncryptedPayloadOffset);
            var computedAuthenticationTag = ComputeAuthenticationTag(
                _decryptionAuthenticationKey!,
                message.Slice(0, AuthenticatedHeaderSize),
                encryptedPayload);

            try
            {
                if (!CryptographicOperations.FixedTimeEquals(computedAuthenticationTag, receivedAuthenticationTag))
                    throw new CryptographicException("UDP message authentication failed.");
            }
            finally
            {
                CryptographicOperations.ZeroMemory(computedAuthenticationTag);
            }

            var initializationVector = message.Slice(1 + SaltSize, InitializationVectorSize);
            using var aes = CreateAes();
            using var decryptor = aes.CreateDecryptor(_decryptionKey!, initializationVector.ToArray());
            return decryptor.TransformFinalBlock(encryptedMessage, EncryptedPayloadOffset, encryptedPayload.Length);
        }

        private static Aes CreateAes()
        {
            var aes = Aes.Create();
            aes.Mode = CipherMode.CBC;
            aes.Padding = PaddingMode.PKCS7;
            return aes;
        }

        private static byte[] ComputeAuthenticationTag(
            byte[] authenticationKey,
            ReadOnlySpan<byte> authenticatedHeader,
            ReadOnlySpan<byte> encryptedPayload)
        {
            using var hmac = IncrementalHash.CreateHMAC(HashAlgorithmName.SHA256, authenticationKey);
            hmac.AppendData(authenticatedHeader);
            hmac.AppendData(encryptedPayload);
            return hmac.GetHashAndReset();
        }

        private void EnsureEncryptionKeys()
        {
            if (_encryptionSalt is not null)
                return;

            _encryptionSalt = RandomNumberGenerator.GetBytes(SaltSize);
            (_encryptionKey, _encryptionAuthenticationKey) = DeriveKeys(_encryptionSalt);
        }

        private void EnsureDecryptionKeys(ReadOnlySpan<byte> salt)
        {
            if (_decryptionSalt is not null && CryptographicOperations.FixedTimeEquals(_decryptionSalt, salt))
                return;

            ClearKey(ref _decryptionSalt);
            ClearKey(ref _decryptionKey);
            ClearKey(ref _decryptionAuthenticationKey);

            _decryptionSalt = salt.ToArray();
            (_decryptionKey, _decryptionAuthenticationKey) = DeriveKeys(_decryptionSalt);
        }

        private (byte[] EncryptionKey, byte[] AuthenticationKey) DeriveKeys(byte[] salt)
        {
            var keyMaterial = Rfc2898DeriveBytes.Pbkdf2(
                _secret,
                salt,
                Pbkdf2IterationCount,
                HashAlgorithmName.SHA256,
                EncryptionKeySize + AuthenticationKeySize);

            try
            {
                return (
                    keyMaterial.AsSpan(0, EncryptionKeySize).ToArray(),
                    keyMaterial.AsSpan(EncryptionKeySize, AuthenticationKeySize).ToArray());
            }
            finally
            {
                CryptographicOperations.ZeroMemory(keyMaterial);
            }
        }

        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;
            CryptographicOperations.ZeroMemory(_secret);
            ClearKey(ref _encryptionSalt);
            ClearKey(ref _encryptionKey);
            ClearKey(ref _encryptionAuthenticationKey);
            ClearKey(ref _decryptionSalt);
            ClearKey(ref _decryptionKey);
            ClearKey(ref _decryptionAuthenticationKey);
        }

        private static void ClearKey(ref byte[]? key)
        {
            if (key is null)
                return;

            CryptographicOperations.ZeroMemory(key);
            key = null;
        }
    }
}