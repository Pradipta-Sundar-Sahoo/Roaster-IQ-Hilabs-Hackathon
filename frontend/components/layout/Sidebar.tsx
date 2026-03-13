"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { motion } from "framer-motion";
import { cn } from "@/lib/utils";
import { MessageSquare, LayoutDashboard, Brain, Activity, Sparkles } from "lucide-react";

const navItems = [
  { href: "/chat", label: "AI Chat", icon: MessageSquare, desc: "Ask questions" },
  { href: "/dashboard", label: "Dashboard", icon: LayoutDashboard, desc: "Pipeline overview" },
  { href: "/memory", label: "Memory", icon: Brain, desc: "Agent knowledge" },
];

export function Sidebar() {
  const pathname = usePathname();

  return (
    <aside className="w-64 shrink-0 flex flex-col bg-white/80 backdrop-blur-xl border-r border-white/20 shadow-sm">
      <div className="px-5 py-6 border-b border-gray-100/80">
        <div className="flex items-center gap-3">
          <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-indigo-500 via-violet-500 to-purple-600 flex items-center justify-center shadow-lg shadow-indigo-500/25">
            <Sparkles className="w-5 h-5 text-white" />
          </div>
          <div>
            <h1 className="text-base font-bold text-gray-900 tracking-tight">RosterIQ</h1>
            <p className="text-[11px] text-gray-500 font-medium">AI Pipeline Intelligence</p>
          </div>
        </div>
      </div>

      <nav className="flex-1 py-5 px-3 space-y-1">
        {navItems.map((item) => {
          const isActive = pathname === item.href || pathname?.startsWith(item.href + "/");
          const Icon = item.icon;
          return (
            <Link key={item.href} href={item.href} className="block relative">
              {isActive && (
                <motion.div
                  layoutId="sidebar-active"
                  className="absolute inset-0 rounded-xl bg-gradient-to-r from-indigo-500/10 to-violet-500/5 border border-indigo-200/50"
                  transition={{ type: "spring", bounce: 0.2, duration: 0.4 }}
                />
              )}
              <div
                className={cn(
                  "relative flex items-center gap-3 px-4 py-3 rounded-xl text-sm font-medium transition-all duration-200",
                  isActive ? "text-indigo-700" : "text-gray-600 hover:text-gray-900 hover:bg-gray-50/80"
                )}
              >
                <div
                  className={cn(
                    "w-8 h-8 rounded-lg flex items-center justify-center shrink-0 transition-colors",
                    isActive ? "bg-indigo-500/15 text-indigo-600" : "bg-gray-100/80 text-gray-500 group-hover:bg-gray-200/80"
                  )}
                >
                  <Icon className="w-4 h-4" />
                </div>
                <div>
                  <div className="leading-tight">{item.label}</div>
                  <div className={cn("text-[11px] font-normal mt-0.5", isActive ? "text-indigo-500" : "text-gray-400")}>
                    {item.desc}
                  </div>
                </div>
              </div>
            </Link>
          );
        })}
      </nav>

      <div className="p-4 mx-3 mb-3 rounded-xl bg-gradient-to-br from-emerald-50/80 to-teal-50/50 border border-emerald-100/60">
        <div className="flex items-center gap-2.5">
          <div className="w-2 h-2 rounded-full bg-emerald-500 animate-pulse" />
          <span className="text-xs font-semibold text-gray-700">Agent Active</span>
        </div>
        <p className="text-[11px] text-gray-500 mt-0.5 pl-3.5">Gemini 2.5 Flash</p>
      </div>
    </aside>
  );
}
