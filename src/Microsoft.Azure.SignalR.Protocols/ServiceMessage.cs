﻿// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Collections.Generic;

namespace Microsoft.Azure.SignalR.Protocol
{
    /// <summary>
    /// Base class of messages between Azure SignalR Service and SDK.
    /// </summary>
    public abstract class ServiceMessage
    {
    }

    /// <summary>
    /// A handshake request message.
    /// </summary>
    public class HandshakeRequestMessage : ServiceMessage
    {
        /// <summary>
        /// Gets or sets the requested protocol version.
        /// </summary>
        public int Version { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="HandshakeRequestMessage"/> class.
        /// </summary>
        /// <param name="version"></param>
        public HandshakeRequestMessage(int version)
        {
            Version = version;
        }
    }

    /// <summary>
    /// A handshake response message.
    /// </summary>
    public class HandshakeResponseMessage : ServiceMessage
    {
        /// <summary>
        /// Gets or sets the optional error message.
        /// </summary>
        public string ErrorMessage { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="HandshakeResponseMessage"/> class.
        /// </summary>
        public HandshakeResponseMessage() : this(string.Empty)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="HandshakeResponseMessage"/> class.
        /// </summary>
        /// <param name="errorMessage">An optional response error message. A <c>null</c> or empty error message indicates a succesful handshake.</param>
        public HandshakeResponseMessage(string errorMessage)
        {
            ErrorMessage = errorMessage;
        }
    }

    /// <summary>
    /// A ping message.
    /// </summary>
    public class PingMessage : ServiceMessage
    {
        /// <summary>
        /// A static ping message.
        /// </summary>
        public static PingMessage Instance = new PingMessage();

        public Dictionary<string, string> Messages { get; set; }
    }

    /// <summary>
    /// A service error message
    /// </summary>
    public class ServiceErrorMessage : ServiceMessage
    {
        /// <summary>
        /// Gets or sets the error message
        /// </summary>
        public string ErrorMessage { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="ServiceErrorMessage"/> class.
        /// </summary>
        /// <param name="errorMessage">An error message.</param>
        public ServiceErrorMessage(string errorMessage)
        {
            ErrorMessage = errorMessage;
        }
    }
}
