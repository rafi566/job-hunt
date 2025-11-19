import './globals.css';

export const metadata = {
  title: 'DataFlow Studio',
  description: 'ETL connectors inspired by Airbyte with a Fivetran-grade UI.',
};

export default function RootLayout({ children }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
