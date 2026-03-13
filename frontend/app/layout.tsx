import type { Metadata } from "next";
import { Inter } from "next/font/google";
import "./globals.css";
import { Sidebar } from "@/components/layout/Sidebar";

const inter = Inter({
  variable: "--font-inter",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "RosterIQ — AI Pipeline Intelligence",
  description: "Autonomous AI agent for healthcare provider roster pipeline analysis",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body className={`${inter.variable} font-sans antialiased text-gray-900 bg-[#f4f6fc] dark:bg-gray-950`}>
        <div className="flex h-screen overflow-hidden">
          <div className="absolute inset-0 bg-gradient-to-br from-indigo-50/40 via-transparent to-violet-50/30 dark:from-indigo-950/20 dark:via-transparent dark:to-violet-950/20 pointer-events-none" aria-hidden />
          <Sidebar />
          <main className="flex-1 overflow-auto relative">{children}</main>
        </div>
      </body>
    </html>
  );
}
