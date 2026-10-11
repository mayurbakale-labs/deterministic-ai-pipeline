import { defineConfig } from 'astro/config';
import mdx from '@astrojs/mdx';
import tailwind from '@astrojs/tailwind';
// 1. Import the React integration
import react from '@astrojs/react';

export default defineConfig({
  site: 'https://mayurbakale-labs.github.io',
  // Served from the root because the repo is named `mayurbakale-labs.github.io` (GitHub Pages org site).
  base: '/',
  
  // 2. Add react() to the integrations array
  integrations: [mdx(), tailwind(), react()],
});