from model.inference import inference

import discord
from discord.ext import commands

import os

class ChatBot(commands.Bot):
    def __init__(self, command_prefix, self_bot=False):
        intents = discord.Intents.default()        
        commands.Bot.__init__(self, command_prefix=command_prefix, self_bot=self_bot, intents=intents)
    
        self.registerEvents()

    async def on_ready(self):
        print("Bot is now Online")
        self.chatbotChannel = self.get_channel(847229394313150516)
        #await self.chatbotChannel.send("Now Online")

    def registerEvents(self):
        @self.event
        async def on_message(message):
            await self.process_commands(message)
            if message.author != self.user:
                if message.channel != self.chatbotChannel:
                    return

                else:
                    try:
                        content = message.content
                        # Send Content to Model
                        # Reply to message with output from Model

                        answers = inference(content)

                        try:
                            await message.channel.send(answers["answers"][answers["best_index"]])
                        except:
                            await message.channel.send(answers["answers"][0])

                    except Exception as e:
                        await message.reply("**ERROR** Please Check Console")
                        print(f"Error: {e}")

            else:
                return
            

if __name__ == "__main__":
    bot = ChatBot(command_prefix="!")
    bot.run(os.getenv("TOKEN"))