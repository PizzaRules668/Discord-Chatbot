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

    async def on_ready(self): # When Bot is ready
        print("Bot is now Online") # Print bot is online

    def registerEvents(self): # Function to Register All Events that were going to user
        @self.event
        async def on_message(message): # Register On Message Event
            await self.process_commands(message)
            if message.author != self.user and not message.content.startswith("!"): # If message not from user
                try:
                    answers = inference(message.content) # Inference on message content  
                    if not self.inVC:
                        try:
                            await message.reply(answers["answers"][answers["best_index"]]) # Try and get the best message
                        except:
                            await message.reply(answers["answers"][0]) # Else Get the first answer

                    elif self.inVC:
                        try:
                            tts = gTTS(text=answers["answers"][answers["best_index"]], lang="en")
                            tts.save("message.mp3")

                        except Exception as e:
                            print(e)
                            tts = gTTS(text=answers["answers"][0], lang="en")
                            tts.save("message.mp3")
                            
                        
                        self.vc.play(discord.FFmpegPCMAudio(source="message.mp3"))
                        os.remove("message.mp3")

                except Exception as e: # If error
                    await message.channel.send("**ERROR** Please Check Console")
                    print(f"Error: {e}")

    def registerCommands(self):
        @self.command()
        async def join(message):
            if message.author.voice is None:
                await message.reply("You Must Be In Voice Channel")

            else:
                self.vc = await message.author.voice.channel.connect()
                self.inVC = True

        @self.command()
        async def leave(message):
            if not self.inVC:
                return

            await message.guild.voice_client.disconnect()

load_dotenv()
if __name__ == "__main__":
    bot = ChatBot(command_prefix="!")
    bot.run(os.getenv("TOKEN"))