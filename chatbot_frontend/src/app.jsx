import ChatBot from "./ChatBot";

function App() {
  return (
    <div className="w-full h-screen bg-gray-50">
      {/* Page Content */}
      <h1 className="text-3xl font-bold p-10">AI-Chatbot Demo</h1>
      
      {/* The ChatBot Widget */}
      <ChatBot />
    </div>
  );
}

export default App;