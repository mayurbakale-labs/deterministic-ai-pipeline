import { z } from 'zod';

export const DataExtractionSchema = z.object({
  sentiment: z.enum(['positive', 'neutral', 'negative']),
  confidenceScore: z.number().min(0).max(1),
  tags: z.array(z.string()),
  summary: z.string().max(280),
  entitiesExtracted: z.array(z.object({
    name: z.string(),
    type: z.string()
  }))
});

export async function runDeterministicPipeline(rawInputText, sdkClient) {
  const maxRetries = 3;
  let attempt = 0;

  while (attempt < maxRetries) {
    try {
      const response = await sdkClient.models.generateContent({
        model: 'gemini-2.5-pro',
        contents: 'Analyze the following incoming core system log data: ' + rawInputText,
        config: {
          temperature: 0.0,
          responseMimeType: 'application/json',
          responseSchema: DataExtractionSchema,
        }
      });

      const rawJsonString = response.text;
      const validatedData = DataExtractionSchema.parse(JSON.parse(rawJsonString));
      return validatedData;

    } catch (error) {
      attempt++;
      console.warn('Pipeline failure detected on execution attempt ' + attempt + '/' + maxRetries + ': ' + error.message);
      if (attempt >= maxRetries) {
        throw new Error('Pipeline Circuit Breaker Tripped: Failed to achieve determinism after ' + maxRetries + ' attempts.');
      }
    }
  }
}
