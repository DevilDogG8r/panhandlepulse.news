import "./globals.css";

export const metadata = {
  title: "Panhandle Pulse",
  description: "Latest news across the Florida Panhandle — updated frequently."
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>
        <div className="container">
          <div className="header">
            <div className="brand">
              <h1>Panhandle Pulse</h1>
              <p>Latest articles ingested from your sources</p>
            </div>
          </div>
          {children}
          <div className="footer">Read-only beta • Powered by your ingestion pipeline</div>
        </div>
      </body>
    </html>
  );
}
