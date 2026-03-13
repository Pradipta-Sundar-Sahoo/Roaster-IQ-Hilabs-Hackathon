"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { cn } from "@/lib/utils";
import { MessageSquare, LayoutDashboard, Brain, Activity } from "lucide-react";

const navItems = [
  { href: "/chat", label: "AI Chat", icon: MessageSquare, desc: "Ask questions" },
  { href: "/dashboard", label: "Dashboard", icon: LayoutDashboard, desc: "Pipeline overview" },
  { href: "/memory", label: "Memory", icon: Brain, desc: "Agent knowledge" },
];

export function Sidebar() {
  const pathname = usePathname();

  return (
    <div className="w-72 bg-white border-r border-gray-100 flex flex-col">
      <div className="px-6 py-6 border-b border-gray-100">
        <div className="flex items-center gap-3">
          <div className="w-11 h-11 rounded-2xl bg-gradient-to-br from-indigo-500 to-blue-600 flex items-center justify-center shadow-md shadow-indigo-200">
            <span className="text-white font-bold text-lg">R</span>
          </div>
          <div>
            <h1 className="text-lg font-bold text-gray-900 tracking-tight">RosterIQ</h1>
            <p className="text-xs text-gray-400 font-medium">AI Pipeline Intelligence</p>
          </div>
        </div>
      </div>

      <nav className="flex-1 p-4 space-y-1.5">
        {navItems.map((item) => {
          const isActive = pathname === item.href || pathname?.startsWith(item.href + "/");
          const Icon = item.icon;
          return (
            <Link
              key={item.href}
              href={item.href}
              className={cn(
                "flex items-center gap-4 px-5 py-4 rounded-2xl text-sm font-semibold transition-all duration-200",
                isActive
                  ? "bg-indigo-50 text-indigo-700 shadow-sm"
                  : "text-gray-500 hover:text-gray-800 hover:bg-gray-50"
              )}
            >
              <Icon className={cn("w-5 h-5", isActive ? "text-indigo-600" : "text-gray-400")} />
              <div>
                <div className="leading-tight">{item.label}</div>
                <div className={cn("text-xs font-normal mt-0.5", isActive ? "text-indigo-400" : "text-gray-400")}>
                  {item.desc}
                </div>
              </div>
            </Link>
          );
        })}
      </nav>

      <div className="p-5 border-t border-gray-100 mx-4 mb-4">
        <div className="flex items-center gap-3">
          <Activity className="w-4 h-4 text-emerald-500" />
          <span className="text-sm font-medium text-gray-700">Agent Active</span>
        </div>
        <p className="text-xs text-gray-400 mt-1 ml-7">Gemini 2.5 Flash</p>
      </div>
    </div>
  );
}
