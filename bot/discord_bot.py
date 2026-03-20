import os
import discord
from discord.ext import commands
import logging

# Initialize intent and bot
intents = discord.Intents.default()
discord_bot = commands.Bot(command_prefix="!", intents=intents)

DISCORD_TOKEN = os.getenv("BRG_DISCORD_TOKEN")
DISCORD_CHANNEL_ID = os.getenv("BRG_DISCORD_CHANNEL_ID")

# You will need to import the project's DB functions here
# e.g., from data.video_store import db

class RequestView(discord.ui.View):
    def __init__(self, video_id: str, channel_id: str):
        # timeout=None ensures buttons persist across bot restarts
        super().__init__(timeout=None) 
        self.video_id = video_id
        self.channel_id = channel_id

    # --- ROW 1: Video Actions ---
    @discord.ui.button(label="Approve Video", style=discord.ButtonStyle.success, row=0, custom_id="btn_app_vid")
    async def approve_vid(self, interaction: discord.Interaction, button: discord.ui.Button):
        # db.approve_video(self.video_id)
        await interaction.response.send_message("✅ Video Approved.", ephemeral=True)
        # Optional: Edit the original message to remove buttons after action
        await interaction.message.edit(view=None)

    @discord.ui.button(label="Deny Video", style=discord.ButtonStyle.danger, row=0, custom_id="btn_deny_vid")
    async def deny_vid(self, interaction: discord.Interaction, button: discord.ui.Button):
        # db.deny_video(self.video_id)
        await interaction.response.send_message("❌ Video Denied.", ephemeral=True)
        await interaction.message.edit(view=None)

    @discord.ui.button(label="Block Video", style=discord.ButtonStyle.secondary, row=0, custom_id="btn_blk_vid")
    async def block_vid(self, interaction: discord.Interaction, button: discord.ui.Button):
        # db.block_video(self.video_id)
        await interaction.response.send_message("🚫 Video permanently blocked.", ephemeral=True)
        await interaction.message.edit(view=None)

    # --- ROW 2: Channel Actions ---
    @discord.ui.button(label="Allow Channel", style=discord.ButtonStyle.primary, row=1, custom_id="btn_all_chan")
    async def allow_chan(self, interaction: discord.Interaction, button: discord.ui.Button):
        # db.allow_channel(self.channel_id)
        await interaction.response.send_message("✅ Channel Allowed. Future videos will auto-approve.", ephemeral=True)
        await interaction.message.edit(view=None)

    @discord.ui.button(label="Block Channel", style=discord.ButtonStyle.danger, row=1, custom_id="btn_blk_chan")
    async def block_chan(self, interaction: discord.Interaction, button: discord.ui.Button):
        # db.block_channel(self.channel_id)
        await interaction.response.send_message("🚫 Channel Blocked. All videos from this channel will be denied.", ephemeral=True)
        await interaction.message.edit(view=None)

@discord_bot.event
async def on_ready():
    logging.info(f"Discord Bot logged in as {discord_bot.user}")

async def send_discord_notification(video_title: str, thumbnail_url: str, video_id: str, channel_name: str, channel_id: str, duration: str):
    """Called by the FastAPI application when a new video is requested."""
    if not DISCORD_TOKEN or not DISCORD_CHANNEL_ID:
        return

    channel = discord_bot.get_channel(int(DISCORD_CHANNEL_ID))
    if not channel:
        logging.error("Discord channel not found. Check BRG_DISCORD_CHANNEL_ID.")
        return

    embed = discord.Embed(
        title="📺 New Video Request", 
        description=f"**{video_title}**\n\n**Channel:** {channel_name}\n**Duration:** {duration}", 
        color=0x3498db
    )
    embed.set_thumbnail(url=thumbnail_url)
    
    view = RequestView(video_id=video_id, channel_id=channel_id)
    await channel.send(embed=embed, view=view)

async def start_discord_bot():
    """Starts the Discord bot background task."""
    if DISCORD_TOKEN:
        await discord_bot.start(DISCORD_TOKEN)
