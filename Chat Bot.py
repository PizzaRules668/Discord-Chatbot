from model.inference import inference # Used to get reply
from discord.ext.commands import Bot # Discord Bot 
from dotenv import load_dotenv # Load Secret Keys
import os # Used to Get Secret Keys

class ChatBot(Bot): # Make Bot
    def __init__(self, command_prefix, self_bot=False):
        Bot.__init__(self, command_prefix=command_prefix, self_bot=self_bot) # Init Bot 
    
        self.registerEvents() # Register All Discord Events that we are going to us (on_message)

    async def on_ready(self): # When Bot is ready
        print("Bot is now Online") # Print bot is online
        self.chatbotChannel = self.get_channel(847229394313150516) # Get channel you want the bot to only use

    def registerEvents(self): # Function to Register All Events that were going to user
        @self.event
        async def on_message(message): # Register On Message Event
            if message.author != self.user: # If message not from user
                try:
                    answers = inference(message.content) # Inference on message content  
                    try:
                        await message.reply(answers["answers"][answers["best_index"]]) # Try and get the best message
                    except:
                        await message.reply(answers["answers"][0]) # Else Get the first answer

                except Exception as e: # If error
                    await message.channel.send("**ERROR** Please Check Console") # 
                    print(f"Error: {e}")

load_dotenv()
if __name__ == "__main__":
    bot = ChatBot(command_prefix="!")
    bot.run(os.getenv("TOKEN"))