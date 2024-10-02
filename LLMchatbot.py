from langchain.llms import OpenAI
from langchain.chains import ConversationChain
from langchain.memory import ConversationBufferMemory
# Set up OpenAI API key
import os
os.environ["OPENAI_API_KEY"] = "your_api_key_here"
# Initialize LLM
llm = OpenAI(temperature=0.7)
# Set up conversation memory
memory = ConversationBufferMemory(memory_key="chat_history")
# Create conversation chain
conversation = ConversationChain(
    llm=llm,
    memory=memory,
    verbose=True
)

# Start conversation
print("Welcome to the chatbot! How can I assist you today?")

while True:
    user_input = input("You: ")
    if user_input.lower() == "exit":
        print("Chatbot: Goodbye!")
        break

    response = conversation.predict(input=user_input)
    print(f"Chatbot: {response}")
