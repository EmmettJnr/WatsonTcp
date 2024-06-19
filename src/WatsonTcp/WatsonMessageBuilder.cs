namespace WatsonTcp
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;

    internal class WatsonMessageBuilder
    {
        #region Internal-Members

        internal ISerializationHelper SerializationHelper
        {
            get => _SerializationHelper;
            set
            {
                if (value == null) throw new ArgumentNullException(nameof(SerializationHelper));
                _SerializationHelper = value;
            }
        }

        internal int ReadStreamBuffer
        {
            get => _ReadStreamBuffer;
            set
            {
                if (value < 1) throw new ArgumentOutOfRangeException(nameof(ReadStreamBuffer));
                _ReadStreamBuffer = value;
            }
        }

        #endregion

        #region Private-Members

        private ISerializationHelper _SerializationHelper = new DefaultSerializationHelper();
        private int _ReadStreamBuffer = 65536;

        #endregion

        #region Constructors-and-Factories

        internal WatsonMessageBuilder()
        {

        }

        #endregion

        #region Internal-Methods

        /// <summary>
        /// Construct a new message to send.
        /// </summary>
        /// <param name="contentLength">The number of bytes included in the stream.</param>
        /// <param name="stream">The stream containing the data.</param>
        /// <param name="syncRequest">Indicate if the message is a synchronous message request.</param>
        /// <param name="syncResponse">Indicate if the message is a synchronous message response.</param>
        /// <param name="expirationUtc">The UTC time at which the message should expire (only valid for synchronous message requests).</param>
        /// <param name="metadata">Metadata to attach to the message.</param>
        internal WatsonMessage ConstructNew(
            long contentLength,
            Stream stream,
            bool syncRequest = false,
            bool syncResponse = false,
            DateTime? expirationUtc = null,
            Dictionary<string, object> metadata = null)
        {
            if (contentLength < 0) throw new ArgumentException("Content length must be zero or greater.");
            if (contentLength > 0)
            {
                if (stream == null || !stream.CanRead)
                {
                    throw new ArgumentException("Cannot read from supplied stream.");
                }
            }

            WatsonMessage msg = new WatsonMessage();
            msg.ContentLength = contentLength;
            msg.DataStream = stream;
            msg.SyncRequest = syncRequest;
            msg.SyncResponse = syncResponse;
            msg.ExpirationUtc = expirationUtc;
            msg.Metadata = metadata;

            return msg;
        }

        /// <summary>
        /// Read from a stream and construct a message.
        /// </summary>
        /// <param name="stream">Stream.</param>
        /// <param name="maxHeaderSize">Maximum header size.</param>
        /// <param name="token">Cancellation token.</param>
        internal async Task<WatsonMessage> BuildFromStream(Stream stream, long maxHeaderSize, 
            CancellationToken token = default)
        {
            if (stream == null) throw new ArgumentNullException(nameof(stream));
            if (!stream.CanRead) throw new ArgumentException("Cannot read from stream.");

            // Read the header size and validate it.
            byte[] data = await ReadByteArray(stream, new byte[4], 0, 4, token).ConfigureAwait(false);
            int headerSize = BitConverter.ToInt32(data, 0);
            if (headerSize < 0 || headerSize > maxHeaderSize)
            {
                throw new IOException($"Invalid header size ({headerSize}), must be more than zero and less " +
                                      $"or equal to {maxHeaderSize} bytes.");
            }
            
            // Read the header bytes and create the message.
            data = await ReadByteArray(stream, new byte[headerSize], 0, headerSize, token).ConfigureAwait(false);
            WatsonMessage msg = GetWatsonMessageFromBytes(data);
            msg.DataStream = stream;
            return msg;
        }

        /// <summary>
        /// Retrieve header bytes for a message.
        /// </summary>
        /// <param name="msg">Watson message.</param>
        /// <returns>Header bytes.</returns>
        internal byte[] GetHeaderBytes(WatsonMessage msg)
        {
            // !!! If you're modifying this method, make sure to update the corresponding
            // !!! GetWatsonMessageFromBytes method to match the changes.
            
            MemoryStream stream = new MemoryStream();
            BinaryWriter writer = new BinaryWriter(stream);

            // Create the byte flag specifying what fields are null.
            byte flag = 0;
            if (msg.PresharedKey != null) flag = (byte)(flag | 0x01);
            if (msg.Metadata != null) flag = (byte)(flag | 0x02);
            if (msg.ExpirationUtc != null) flag = (byte)(flag | 0x04);
            if (msg.Status == MessageStatus.RegisterClient) flag = (byte)(flag | 0x08);
            // Write the flags at the start of the message.
            writer.Write(flag);
            
            // Write the content of the header.
            writer.Write(msg.ContentLength);
            if (msg.PresharedKey != null) writer.Write(msg.PresharedKey);
            writer.Write((int)msg.Status);
            // TODO: Metadata.
            writer.Write(msg.SyncRequest);
            writer.Write(msg.SyncResponse);
            writer.Write(msg.TimestampUtc.Ticks);
            if (msg.ExpirationUtc != null) writer.Write(msg.ExpirationUtc.Value.Ticks);
            writer.Write(msg.ConversationGuid.ToByteArray());
            if (msg.Status == MessageStatus.RegisterClient) writer.Write(msg.SenderGuid.ToByteArray());
            
            // Create the final message, making sure to insert the size of the
            // header at the start.
            byte[] headerBytes = stream.ToArray();
            byte[] headerSize = BitConverter.GetBytes(headerBytes.Length);
            return WatsonCommon.AppendBytes(headerSize, headerBytes);
        }
        
        internal WatsonMessage GetWatsonMessageFromBytes(byte[] data)
        {
            // !!! If you're modifying this method, make sure to update the corresponding
            // !!! GetHeaderBytes method to match the changes.
            
            MemoryStream stream = new MemoryStream(data);
            BinaryReader reader = new BinaryReader(stream);
            
            // Get the data.
            byte flag = reader.ReadByte();
            long contentLength = reader.ReadInt64();
            byte[] presharedKey = (flag & 0x01) != 0 ? reader.ReadBytes(16) : null;
            MessageStatus status = (MessageStatus)reader.ReadInt32();
            // TODO: Metadata.
            //Dictionary<string, object> metadata = (flag & 0x02) != 0 ? null : null;
            bool syncRequest = reader.ReadBoolean();
            bool syncResponse = reader.ReadBoolean();
            DateTime timestampUtc = new DateTime(reader.ReadInt64());
            DateTime? expirationUtc = (flag & 0x04) != 0 ? new DateTime(reader.ReadInt64()) : (DateTime?)null;
            Guid conversationGuid = new Guid(reader.ReadBytes(16));
            Guid senderGuid = (flag & 0x08) != 0 ? new Guid(reader.ReadBytes(16)) : default;
            
            // Create the message and return it.
            return new WatsonMessage
            {
                ContentLength = contentLength,
                PresharedKey = presharedKey,
                Status = status,
                SyncRequest = syncRequest,
                SyncResponse = syncResponse,
                TimestampUtc = timestampUtc,
                ExpirationUtc = expirationUtc,
                ConversationGuid = conversationGuid,
                SenderGuid = senderGuid
            };
        }

        /// <summary>
        /// A simple method to read a specified number of bytes from a stream.
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="data"></param>
        /// <param name="start"></param>
        /// <param name="size"></param>
        /// <param name="token"></param>
        /// <exception cref="IOException"></exception>
        internal async Task<byte[]> ReadByteArray(Stream stream, byte[] data, int start, int size, CancellationToken token)
        {
            if (start < 0) throw new ArgumentOutOfRangeException(nameof(start), "Start must be zero or greater.");
            if (size < 1) throw new ArgumentOutOfRangeException(nameof(size), "Size must be greater than zero.");
            if (data.Length < start + size) throw new ArgumentException("Data array is too small.");
            
            int read = 0;
            while (read < size)
            {
                int readNow = await stream.ReadAsync(data, start + read, size - read, token).ConfigureAwait(false);
                if (readNow == 0) throw new IOException("End of stream reached.");
                read += readNow;
            }
            
            return data;
        }

        #endregion

        #region Private-Methods

        #endregion
    }
}
