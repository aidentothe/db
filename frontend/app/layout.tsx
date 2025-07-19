import type { Metadata } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import "./globals.css";
import Chatbot from "../components/Chatbot";

const geistSans = Geist({
  variable: "--font-geist-sans",
  subsets: ["latin"],
});

const geistMono = Geist_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "Spark DB",
  description: "A powerful database management tool with Apache Spark integration for ETL pipelines, SQL querying, and data processing",
  keywords: ["Apache Spark", "Database", "ETL", "SQL", "Data Processing", "Analytics"],
  authors: [{ name: "Apache Spark DB Team" }],
  creator: "Apache Spark Database Manager",
  publisher: "Apache Spark DB Team",
  formatDetection: {
    email: false,
    address: false,
    telephone: false,
  },
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body
        className={`${geistSans.variable} ${geistMono.variable} antialiased`}
      >
        {children}
        <Chatbot position="bottom-right" />
      </body>
    </html>
  );
}
