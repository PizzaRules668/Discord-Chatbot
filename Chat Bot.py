from model.inference import inference # Used to get reply
from discord.ext.commands import Bot # Discord Bot 
from dotenv import load_dotenv # Load Secret Keys
import discord # Base Discord
import pyttsx3 # Used For TTX
import json # Used for loading settings
import os # Used to Get Secret Keys

class ChatBot(Bot): # Make Bot
    def __init__(self, command_prefix, self_bot=False):
        Bot.__init__(self, command_prefix=command_prefix, self_bot=self_bot) # Init Bot 
    
        self.registerEvents() # Register All Discord Events that we are going to use (on_message)
        self.registerCommands() # Register All Discord Commands that we are going to use (join)

        self.engine = pyttsx3.init() # Init pyttsx3 Engine

        self.inVC = False
        self.vc = None

    async def on_ready(self): # When Bot is ready
        print("Bot is now Online") # Print bot is online

    def registerEvents(self): # Function to Register All Events that were going to user
        @self.event
        async def on_message(message): # Register On Message Event
            await self.process_commands(message)
            if message.author != self.user: # If message not from user
                try:
                    answers = inference(message.content) # Inference on message content  
                    if not self.inVC:
                        try:
                            await message.reply(answers["answers"][answers["best_index"]]) # Try and get the best message
                        except:
                            await message.reply(answers["answers"][0]) # Else Get the first answer

                    elif self.inVC:
                        try:
                            self.engine.save_to_file(answers["answers"][answers["best_index"]], "message.wav") # Try and get the best message

                        except:
                            self.engine.save_to_file(answers["answers"][0], "message.wav") # Else Get the first answer
                        
                        self.vc.play(discord.FFmpegPCMAudio(source="message.wav"))

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
            if self.inVC:
                pass

load_dotenv()
if __name__ == "__main__":
    bot = ChatBot(command_prefix="!")
    bot.run(os.getenv("TOKEN"))