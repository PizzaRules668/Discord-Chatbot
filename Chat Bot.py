from model.inference import inference # Used to get reply
from discord.ext.commands import Bot # Discord Bot 
from dotenv import load_dotenv # Load Secret Keys
from gtts import gTTS # Used For TTS
import discord # Base Discord
import json # Used for loading settings
import os # Used to Get Secret Keys

class ChatBot(Bot): # Make Bot
    def __init__(self, command_prefix, self_bot=False):
        Bot.__init__(self, command_prefix=command_prefix, self_bot=self_bot) # Init Bot 

        self.registerEvents() # Register All Discord Events that we are going to use (on_message)
        self.registerCommands() # Register All Discord Commands that we are going to use (join)

        self.inVC = False
        self.vc = None
        self.userInVC = None

    async def on_ready(self): # When Bot is ready
        print("Bot is now Online") # Print bot is online

    def registerEvents(self): # Function to Register All Events that were going to user
        @self.event
        async def on_message(message): # Register On Message Event
            await self.process_commands(message)
            if message.author != self.user and not message.content.startswith("!"): # If message not from user
                try:
                    answers = inference(message.content) # Inference on message content  
                    if self.inVC and self.userInVC == message.author: # If from user that is in vc
                        try:
                            tts = gTTS(text=answers["answers"][answers["best_index"]], lang="en") # Turn into tts object
                            tts.save("message.mp3") # Save as .MP3

                        except Exception as e:
                            tts = gTTS(text=answers["answers"][0], lang="en") # Turn into tts object
                            tts.save("message.mp3") # Save as .MP3
                        
                        self.vc.play(discord.FFmpegPCMAudio(source="message.mp3")) # Play the .MP3 file that we saved

                    else:
                        try:
                            await message.reply(answers["answers"][answers["best_index"]]) # Try and get the best message
                        except:
                            await message.reply(answers["answers"][0]) # Try and get the first message

                except Exception as e: # If error
                    await message.channel.send("**ERROR** Please Check Console")
                    print(f"Error: {e}")

    def registerCommands(self): # Register all commands
        @self.command()
        async def join(message): 
            if message.author.voice is None: # Make sure the user is in the voice channel
                await message.reply("You Must Be In Voice Channel")

            else:
                self.vc = await message.author.voice.channel.connect() # Connect bot to the voice channel
                self.userInVC = message.author # Set user that is also in the voice channel
                self.inVC = True # 

        @self.command()
        async def leave(message):
            if not self.inVC: # Check to see if it in a voice channel
                return

            self.vc = None # Reset All voice channel related variables
            self.inVC = False
            self.userInVC = None
            await message.guild.voice_client.disconnect() # Leave voice channel

load_dotenv() # Init module to load from .env
if __name__ == "__main__":
    bot = ChatBot(command_prefix="!") # Init bot with prefix of !
    bot.run(os.getenv("TOKEN")) # Start bot with token grabbed from .env