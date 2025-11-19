import './globals.css';
import type { Metadata } from 'next';

export const metadata: Metadata = {
  title: 'DataFlow Studio',
  description: 'ETL connectors inspired by Airbyte with a Fivetran-grade UI.',
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
