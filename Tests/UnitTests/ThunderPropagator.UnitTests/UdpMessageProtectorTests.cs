using System.Security.Cryptography;
using System.Text;
using ThunderPropagator.Feeviders.UdpClient.SharedKernel;

namespace ThunderPropagator.UnitTests
{
    public class UdpMessageProtectorTests
    {
        [Fact]
        public void ProtectAndUnprotect_ShouldRoundTripWithUnicodeSecret()
        {
            const string secret = "correct horse battery staple 🔐";
            var plainData = Encoding.UTF8.GetBytes("encrypted UDP payload");
            using var sender = new UdpMessageProtector(secret);
            using var receiver = new UdpMessageProtector(secret);

            var encryptedData = sender.Protect(plainData);
            var decryptedData = receiver.Unprotect(encryptedData);

            Assert.Equal(plainData, decryptedData);
        }

        [Fact]
        public void Protect_ShouldUseRandomSaltForEachSender()
        {
            using var firstSender = new UdpMessageProtector("shared secret");
            using var secondSender = new UdpMessageProtector("shared secret");
            var plainData = Encoding.UTF8.GetBytes("payload");

            var firstEncryptedData = firstSender.Protect(plainData);
            var secondEncryptedData = secondSender.Protect(plainData);

            Assert.False(firstEncryptedData.AsSpan(1, 16).SequenceEqual(secondEncryptedData.AsSpan(1, 16)));
        }

        [Fact]
        public void Unprotect_ShouldRejectTamperedInitializationVector()
        {
            const string secret = "shared secret";
            using var sender = new UdpMessageProtector(secret);
            using var receiver = new UdpMessageProtector(secret);
            var encryptedData = sender.Protect(Encoding.UTF8.GetBytes("payload"));
            encryptedData[20] ^= 0x01;

            Assert.Throws<CryptographicException>(() => receiver.Unprotect(encryptedData));
        }

        [Fact]
        public void Unprotect_ShouldRejectWrongSecret()
        {
            using var sender = new UdpMessageProtector("sender secret");
            using var receiver = new UdpMessageProtector("receiver secret");
            var encryptedData = sender.Protect(Encoding.UTF8.GetBytes("payload"));

            Assert.Throws<CryptographicException>(() => receiver.Unprotect(encryptedData));
        }
    }
}