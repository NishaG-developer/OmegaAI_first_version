import { useEffect, useState, useRef } from "react";
import { Send, MessageSquare, Loader2, Minus, MoreHorizontal, Box } from 'lucide-react';

const BASE_URL = "http://localhost:8000";

export default function ChatBot() {
  const [sessionId, setSessionId] = useState(null); 
  const [messages, setMessages] = useState([
    { sender: "bot", text: "Hello! I'm OmegaAI. How can I help you today?" }
  ]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [isOpen, setIsOpen] = useState(false); 
  const chatEndRef = useRef(null);

  // Auto-scroll
  useEffect(() => {
    chatEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, isOpen]);

  // Session Start Logic
  useEffect(() => {
    async function startSession() {
      try {
        const res = await fetch(`${BASE_URL}/session/start`, { method: "POST" });
        const data = await res.json();
        setSessionId(data.session_id);
      } catch (err) {
        console.error("Failed to start session", err);
      }
    }
    if (!sessionId) startSession();
  }, []);

  async function sendMessage() {
    if (!input.trim() || loading || !sessionId) return;
    
    const userMsg = { sender: "user", text: input.trim() };
    setMessages(prev => [...prev, userMsg]);
    setLoading(true);
    setInput("");

    try {
      const res = await fetch(`${BASE_URL}/smart`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ message: userMsg.text, session_id: sessionId }),
      });
      const data = await res.json();
      setMessages(prev => [...prev, { sender: "bot", text: data.reply }]);
    } catch (err) {
      setMessages(prev => [...prev, { sender: "bot", text: "⚠️ I couldn't connect to the server." }]);
    }
    setLoading(false);
  }

  // --- 1. CLOSED STATE ---
  if (!isOpen) {
    return (
      <div className="fixed bottom-6 right-6 z-50 flex flex-col items-end gap-2">
        {/* The Text Bubble */}
        <div className="bg-white px-5 py-3 rounded-2xl shadow-xl border border-gray-100 mb-1 animate-bounce-slight cursor-pointer" onClick={() => setIsOpen(true)}>
          <span className="text-gray-800 font-medium">Hi there</span>
        </div>

        {/* The Toggle Button (Indigo) */}
        <button
          onClick={() => setIsOpen(true)}
          className="p-4 bg-indigo-600 text-white rounded-full shadow-2xl hover:bg-indigo-700 transition duration-300 flex items-center justify-center"
        >
          <MessageSquare size={28} />
        </button>
      </div>
    );
  }

  // --- 2. OPEN STATE ---
  return (
    <div className="fixed bottom-6 right-6 z-50 w-full max-w-[380px]">
      <div className="bg-white shadow-2xl rounded-3xl flex flex-col h-[600px] border border-gray-100 overflow-hidden font-sans">
        
        {/* === HEADER (The Pill) === */}
        <div className="flex items-center justify-between px-6 py-4 bg-white border-b border-gray-50">
          {/* Left: Menu Dots */}
          <MoreHorizontal className="text-gray-400 cursor-pointer hover:text-gray-600" size={24} />

          {/* Center: The Pill */}
          <div className="flex items-center gap-2 px-4 py-2 bg-white border border-gray-200 rounded-full shadow-sm select-none">
             {/* Logo Icon */}
             <Box size={18} className="text-orange-500" />
             
             {/* Name */}
             <span className="font-bold text-gray-800 text-sm">OmegaAI</span>
          </div>

          {/* Right: Minimize Dash */}
          <button onClick={() => setIsOpen(false)} className="text-gray-400 hover:text-gray-600 p-1">
            <Minus size={28} strokeWidth={1.5} />
          </button>
        </div>

        {/* Messages Area */}
        <div className="flex-1 overflow-y-auto p-5 space-y-6 bg-white scrollbar-hide">
          {messages.map((msg, idx) => (
            <div key={idx} className={`flex ${msg.sender === "user" ? "justify-end" : "justify-start"}`}>
              {/* Bot Avatar */}
              {msg.sender === 'bot' && (
                 <div className="w-8 h-8 rounded-full bg-orange-50 border border-orange-100 flex items-center justify-center mr-2 mt-1">
                    <Box size={14} className="text-orange-500" />
                 </div>
              )}
              
              <div className={`px-5 py-3 max-w-[80%] text-[15px] leading-relaxed shadow-sm whitespace-pre-wrap ${
                msg.sender === "user" 
                  ? "bg-[#2b3b7c] text-white rounded-2xl rounded-br-sm" 
                  : "bg-white text-gray-800 border border-gray-100 rounded-2xl rounded-tl-sm shadow-sm"
              }`}>
                {msg.text}
              </div>
            </div>
          ))}
          
          {loading && (
             <div className="flex items-center gap-2 text-gray-400 text-sm ml-2">
                <div className="w-8 h-8 rounded-full bg-gray-50 flex items-center justify-center">
                   <Loader2 className="animate-spin text-gray-400" size={14} /> 
                </div>
             </div>
          )}
          <div ref={chatEndRef}></div>
        </div>

        {/* Input Area */}
        <div className="p-5 bg-white">
          <div className="flex items-center gap-2 bg-gray-50 border border-gray-200 rounded-full px-2 py-2">
            <input
              type="text"
              className="flex-1 px-4 bg-transparent outline-none text-gray-700 text-sm"
              placeholder={sessionId ? "Type a message..." : "Connecting..."}
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={(e) => e.key === "Enter" && sendMessage()}
              disabled={loading}
            />
            <button
              onClick={sendMessage}
              className={`p-2 rounded-full transition duration-200 flex items-center justify-center ${
                input.trim() 
                  ? "bg-[#2b3b7c] text-white shadow-md hover:bg-blue-900" 
                  : "bg-gray-200 text-gray-400 cursor-not-allowed"
              }`}
              disabled={!input.trim() || loading}
            >
              <Send size={18} />
            </button>
          </div>
          <div className="text-center mt-2">
             <span className="text-[10px] text-gray-400">Powered by OmegaCube</span>
          </div>
        </div>

      </div>
    </div>
  );
}