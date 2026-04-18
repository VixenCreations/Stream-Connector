using System;

public class CPHInline
{
    public bool Execute()
    {
        try
        {
            // ─────────────────────────────────────────────
            // CONFIG (can be overridden by Streamer.bot args)
            // These values define how the event will be routed
            // inside Stream Connector.
            // ─────────────────────────────────────────────

            // Integration namespace used by Stream Connector for routing
            string provider  = args.ContainsKey("provider")  ? args["provider"].ToString()  : "streamconnector";

            // Hook identifier used to select the automation chain 
            string commandId = args.ContainsKey("commandId") ? args["commandId"].ToString() : "test"; //Change This from "test" to the name of the chain

            // Typed event category. Must match Stream Connector EVENT_PARSERS key.
            string eventType = args.ContainsKey("eventType") ? args["eventType"].ToString() : "external_hook";

            // Optional user context
            string user     = args.ContainsKey("user") ? args["user"].ToString() : "unknown";

            // Raw message or payload text (e.g. chat message, command input, etc.)
            string rawInput = args.ContainsKey("rawInput") ? args["rawInput"].ToString() : "";

            // ─────────────────────────────────────────────
            // BUILD JSON PAYLOAD
            // Streamer.bot requires this to be a STRING, not
            // an object. This payload is consumed by Stream
            // Connector's WebSocket listener and routed into:
            //
            // execute_external_hook(provider, commandId, context)
            // ─────────────────────────────────────────────

            string json =
                "{"
                + "\"type\":\"" + Escape(eventType) + "\","       // Event type (used for parser selection)
                + "\"provider\":\"" + Escape(provider) + "\","    // Integration namespace
                + "\"commandId\":\"" + Escape(commandId) + "\","  // Hook ID for routing
                + "\"context\":{"                                 // Arbitrary payload forwarded to the hook
                    + "\"user\":\"" + Escape(user) + "\","        // User associated with the event
                    + "\"message\":\"" + Escape(rawInput) + "\"," // Raw input / message text
                    + "\"source\":\"streamerbot\","               // Event origin identifier
                    + "\"timestamp\":\"" + DateTime.UtcNow.ToString("o") + "\"" // ISO8601 UTC timestamp
                + "}"
                + "}";

            // ─────────────────────────────────────────────
            // SEND
            // Broadcasts the JSON payload over Streamer.bot's
            // internal WebSocket layer. This is fire-and-forget.
            // ─────────────────────────────────────────────

            CPH.WebsocketBroadcastJson(json);

            // Log success to Streamer.bot console for visibility
            CPH.LogInfo(
                "[StreamConnector] WS broadcast sent → "
                + "eventType=" + eventType
                + " provider=" + provider
                + " commandId=" + commandId
            );

            return true;
        }
        catch (Exception ex)
        {
            // Catch and log any failures to avoid breaking the action chain
            CPH.LogError("[StreamConnector] WS broadcast ERROR: " + ex.ToString());
            return false;
        }
    }

    // ─────────────────────────────────────────────
    // Escape helper
    // Ensures the generated JSON is always valid by
    // escaping characters that would break parsing.
    // ─────────────────────────────────────────────
    private string Escape(string input)
    {
        if (string.IsNullOrEmpty(input)) return "";

        return input
            .Replace("\\", "\\\\")  // Escape backslashes
            .Replace("\"", "\\\"")  // Escape quotes
            .Replace("\n", "\\n")   // Normalize newlines
            .Replace("\r", "\\r");  // Normalize carriage returns
    }
}
