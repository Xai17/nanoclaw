import {
  Client,
  GatewayIntentBits,
  TextChannel,
  Message,
  ChannelType,
} from 'discord.js';

import { logger } from '../logger.js';
import { Channel, OnChatMetadata, OnInboundMessage, RegisteredGroup } from '../types.js';

const DISCORD_JID_PREFIX = 'discord:';

export function channelIdToJid(channelId: string): string {
  return `${DISCORD_JID_PREFIX}${channelId}`;
}

export function jidToChannelId(jid: string): string {
  return jid.slice(DISCORD_JID_PREFIX.length);
}

export interface DiscordChannelOpts {
  token: string;
  onMessage: OnInboundMessage;
  onChatMetadata: OnChatMetadata;
  registeredGroups: () => Record<string, RegisteredGroup>;
}

export class DiscordChannel implements Channel {
  name = 'discord';

  private client: Client;
  private connected = false;
  private opts: DiscordChannelOpts;

  constructor(opts: DiscordChannelOpts) {
    this.opts = opts;
    this.client = new Client({
      intents: [
        GatewayIntentBits.Guilds,
        GatewayIntentBits.GuildMessages,
        GatewayIntentBits.MessageContent,
      ],
    });
  }

  async connect(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.client.once('ready', () => {
        this.connected = true;
        logger.info({ tag: this.client.user?.tag }, 'Connected to Discord');
        resolve();
      });

      this.client.on('messageCreate', async (msg: Message) => {
        // Ignore bot messages
        if (msg.author.bot) return;

        const jid = channelIdToJid(msg.channelId);
        const timestamp = msg.createdAt.toISOString();
        const channelName =
          msg.channel.type === ChannelType.GuildText
            ? (msg.channel as TextChannel).name
            : undefined;

        this.opts.onChatMetadata(jid, timestamp, channelName, 'discord', false);

        const groups = this.opts.registeredGroups();
        if (!groups[jid]) return;

        const content = msg.content;
        if (!content.trim()) return;

        this.opts.onMessage(jid, {
          id: msg.id,
          chat_jid: jid,
          sender: msg.author.id,
          sender_name: msg.member?.displayName ?? msg.author.username,
          content,
          timestamp,
          is_from_me: false,
          is_bot_message: false,
        });
      });

      this.client.on('disconnect', () => {
        this.connected = false;
        logger.warn('Discord disconnected');
      });

      this.client.login(this.opts.token).catch(reject);
    });
  }

  async sendMessage(jid: string, text: string): Promise<void> {
    const channelId = jidToChannelId(jid);
    try {
      const channel = await this.client.channels.fetch(channelId);
      if (!channel || !channel.isTextBased()) {
        logger.error({ jid }, 'Discord channel not found or not text-based');
        return;
      }
      for (const chunk of splitMessage(text)) {
        await (channel as TextChannel).send(chunk);
      }
      logger.info({ jid, length: text.length }, 'Discord message sent');
    } catch (err) {
      logger.error({ jid, err }, 'Failed to send Discord message');
    }
  }

  isConnected(): boolean {
    return this.connected;
  }

  ownsJid(jid: string): boolean {
    return jid.startsWith(DISCORD_JID_PREFIX);
  }

  async disconnect(): Promise<void> {
    this.connected = false;
    await this.client.destroy();
  }
}

function splitMessage(text: string, maxLength = 2000): string[] {
  if (text.length <= maxLength) return [text];
  const chunks: string[] = [];
  let remaining = text;
  while (remaining.length > 0) {
    if (remaining.length <= maxLength) {
      chunks.push(remaining);
      break;
    }
    let splitAt = remaining.lastIndexOf('\n', maxLength);
    if (splitAt <= 0) splitAt = maxLength;
    chunks.push(remaining.slice(0, splitAt));
    remaining = remaining.slice(splitAt).trimStart();
  }
  return chunks;
}
