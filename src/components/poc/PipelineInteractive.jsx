import React, { useState } from 'react';
import { z } from 'zod';

// 1. Your exact Zod Schema from pipeline.js
const DataExtractionSchema = z.object({
  sentiment: z.enum(['positive', 'neutral', 'negative']),
  confidenceScore: z.number().min(0).max(1),
  tags: z.array(z.string()),
  summary: z.string().max(280),
  entitiesExtracted: z.array(z.object({
    name: z.string(),
    type: z.string()
  }))
});

export default function PipelineInteractive() {
  const [inputText, setInputText] = useState('User logged in from new IP address: 192.168.1.45 at 03:00 AM.');
  const [logs, setLogs] = useState([]);
  const [output, setOutput] = useState(null);
  const [isExecuting, setIsExecuting] = useState(false);

  const addLog = (message, type = 'info') => {
    setLogs(prev => [...prev, { time: new Date().toLocaleTimeString(), message, type }]);
  };

  // 2. Mock SDK to simulate LLM behavior securely in the browser
  const mockSdkClient = {
    models: {
      generateContent: async () => {
        // Simulate network latency (800ms - 1500ms)
        await new Promise(res => setTimeout(res, 800 + Math.random() * 700));
        
        // Randomly simulate an LLM hallucination/failure (30% chance) to trigger the retry logic
        if (Math.random() < 0.3) {
           throw new Error("Unexpected token '}' in JSON at position 104");
        }

        // Return a successful, schema-compliant mock response
        return {
          text: JSON.stringify({
            sentiment: "neutral",
            confidenceScore: 0.88,
            tags: ["security", "auth", "anomaly"],
            summary: "A user authenticated from a potentially unrecognized IP address during off-hours.",
            entitiesExtracted: [
              { name: "192.168.1.45", type: "IP_ADDRESS" },
              { name: "03:00 AM", type: "TIMESTAMP" }
            ]
          })
        };
      }
    }
  };

  // 3. Your exact runDeterministicPipeline logic, adapted for React state
  const runPipeline = async () => {
    setIsExecuting(true);
    setLogs([]);
    setOutput(null);
    addLog('Initializing deterministic pipeline...');

    const maxRetries = 3;
    let attempt = 0;
    let success = false;

    while (attempt < maxRetries && !success) {
      try {
        addLog(`Attempt ${attempt + 1}: Calling Gemini 2.5 Pro with Temperature 0.0...`);
        
        const response = await mockSdkClient.models.generateContent({
          model: 'gemini-2.5-pro',
          contents: 'Analyze the following incoming core system log data: ' + inputText,
          config: {
            temperature: 0.0,
            responseMimeType: 'application/json',
            responseSchema: DataExtractionSchema,
          }
        });

        addLog(`Attempt ${attempt + 1}: Response received, applying Zod validation...`);
        const rawJsonString = response.text;
        
        // Validates against the Zod schema
        const validatedData = DataExtractionSchema.parse(JSON.parse(rawJsonString));
        
        addLog('Validation successful. Pipeline execution complete.', 'success');
        setOutput(validatedData);
        success = true;

      } catch (error) {
        attempt++;
        addLog(`Pipeline failure on attempt ${attempt}/${maxRetries}: ${error.message}`, 'error');
        if (attempt >= maxRetries) {
          addLog(`Circuit Breaker Tripped: Failed after ${maxRetries} attempts.`, 'error');
        }
      }
    }
    setIsExecuting(false);
  };

  return (
    <div className="my-10 p-6 bg-slate-900 rounded-xl border border-slate-700 shadow-xl font-sans not-prose">
      <h3 className="text-xl font-semibold text-slate-100 mb-4">Live Pipeline Visualizer</h3>
      
      <div className="mb-4">
        <label className="block text-sm font-medium text-slate-400 mb-2">Simulated Core System Log Input</label>
        <textarea 
          className="w-full bg-slate-950 text-slate-300 border border-slate-700 rounded-lg p-3 focus:outline-none focus:border-blue-500 transition-colors font-mono text-sm"
          rows="3"
          value={inputText}
          onChange={(e) => setInputText(e.target.value)}
          disabled={isExecuting}
        />
      </div>

      <button 
        onClick={runPipeline}
        disabled={isExecuting}
        className="w-full bg-blue-600 hover:bg-blue-500 text-white font-medium py-2 px-4 rounded-lg transition-colors disabled:opacity-50 mb-6"
      >
        {isExecuting ? 'Executing Pipeline...' : 'Run Deterministic Execution'}
      </button>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {/* Execution Logs Terminal */}
        <div className="bg-slate-950 border border-slate-800 rounded-lg p-4 h-64 overflow-y-auto">
          <h4 className="text-xs uppercase tracking-wider text-slate-500 mb-3 font-semibold">Execution Traces</h4>
          <div className="space-y-2 font-mono text-xs">
            {logs.map((log, i) => (
              <div key={i} className={`flex gap-2 ${log.type === 'error' ? 'text-red-400' : log.type === 'success' ? 'text-green-400' : 'text-slate-400'}`}>
                <span className="opacity-50 shrink-0">[{log.time}]</span>
                <span>{log.message}</span>
              </div>
            ))}
            {logs.length === 0 && <span className="text-slate-600">Waiting for execution...</span>}
          </div>
        </div>

        {/* JSON Output Viewer */}
        <div className="bg-slate-950 border border-slate-800 rounded-lg p-4 h-64 overflow-y-auto">
          <h4 className="text-xs uppercase tracking-wider text-slate-500 mb-3 font-semibold">Validated Output (Zod)</h4>
          {output ? (
            <pre className="text-pink-400 font-mono text-xs overflow-x-auto">
              {JSON.stringify(output, null, 2)}
            </pre>
          ) : (
            <div className="text-slate-600 font-mono text-xs">Awaiting strict schema compliance...</div>
          )}
        </div>
      </div>
    </div>
  );
}