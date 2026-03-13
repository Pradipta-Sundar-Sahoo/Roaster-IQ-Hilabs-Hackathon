"use client";

import { useEffect, useState } from "react";
import { getEpisodicMemory, getProceduralMemory, getSemanticMemory } from "@/lib/api";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Brain, Clock, Cog, BookOpen, ArrowRight } from "lucide-react";

export default function MemoryPage() {
  const [episodic, setEpisodic] = useState<{ episodes: Record<string, unknown>[]; state_changes: Record<string, unknown>[] } | null>(null);
  const [procedural, setProcedural] = useState<Record<string, unknown> | null>(null);
  const [semantic, setSemantic] = useState<Record<string, unknown> | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    Promise.all([getEpisodicMemory(), getProceduralMemory(), getSemanticMemory()])
      .then(([ep, proc, sem]) => {
        setEpisodic(ep as typeof episodic);
        setProcedural(proc as typeof procedural);
        setSemantic(sem as typeof semantic);
      })
      .catch(console.error)
      .finally(() => setLoading(false));
  }, []);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-full bg-[#f8f9fb]">
        <div className="flex flex-col items-center gap-3">
          <div className="flex gap-1.5">
            <div className="w-2.5 h-2.5 bg-indigo-500 rounded-full animate-bounce" />
            <div className="w-2.5 h-2.5 bg-indigo-400 rounded-full animate-bounce [animation-delay:0.15s]" />
            <div className="w-2.5 h-2.5 bg-indigo-300 rounded-full animate-bounce [animation-delay:0.3s]" />
          </div>
          <p className="text-gray-400 text-sm">Loading memory stores...</p>
        </div>
      </div>
    );
  }

  return (
    <div className="p-8 space-y-8 bg-[#f8f9fb] min-h-screen">
      <div className="flex items-center gap-3">
        <Brain className="w-5 h-5 text-indigo-500" />
        <div>
          <h2 className="text-xl font-bold text-gray-900">Memory Architecture</h2>
          <p className="text-sm text-gray-400">
            Explore the three memory types powering RosterIQ&apos;s intelligence
          </p>
        </div>
      </div>

      <Tabs defaultValue="episodic">
        <TabsList className="bg-white border border-gray-100 rounded-2xl p-1.5 shadow-sm">
          <TabsTrigger value="episodic" className="rounded-xl px-6 py-3 text-sm font-semibold data-[state=active]:bg-indigo-50 data-[state=active]:text-indigo-700 data-[state=active]:shadow-none">
            <Clock className="w-4 h-4 mr-2" />
            Episodic Memory
          </TabsTrigger>
          <TabsTrigger value="procedural" className="rounded-xl px-6 py-3 text-sm font-semibold data-[state=active]:bg-indigo-50 data-[state=active]:text-indigo-700 data-[state=active]:shadow-none">
            <Cog className="w-4 h-4 mr-2" />
            Procedural Memory
          </TabsTrigger>
          <TabsTrigger value="semantic" className="rounded-xl px-6 py-3 text-sm font-semibold data-[state=active]:bg-indigo-50 data-[state=active]:text-indigo-700 data-[state=active]:shadow-none">
            <BookOpen className="w-4 h-4 mr-2" />
            Semantic Memory
          </TabsTrigger>
        </TabsList>

        {/* Episodic Memory */}
        <TabsContent value="episodic" className="mt-6">
          <div className="grid grid-cols-2 gap-6">
            <Card className="bg-white border-0 ring-1 ring-gray-100 p-6 rounded-2xl shadow-sm">
              <h3 className="text-sm font-bold text-gray-800 mb-4">
                Past Investigations
                <Badge className="ml-2 bg-indigo-50 text-indigo-600 border-indigo-100 text-xs">
                  {episodic?.episodes?.length || 0}
                </Badge>
              </h3>
              <ScrollArea className="h-[500px]">
                <div className="space-y-3">
                  {(episodic?.episodes || []).map((ep: Record<string, unknown>, i: number) => (
                    <div key={i} className="p-4 rounded-xl bg-gray-50/80 border border-gray-100 space-y-2 hover:bg-gray-50 transition-colors">
                      <div className="flex items-center gap-2">
                        <Badge variant="outline" className="text-xs border-gray-200 text-gray-500">
                          {String(ep.intent || "general")}
                        </Badge>
                        <span className="text-xs text-gray-400">{String(ep.timestamp || "").slice(0, 19)}</span>
                      </div>
                      <p className="text-sm text-gray-700 font-medium">{String(ep.query || "")}</p>
                      {ep.findings_summary ? (
                        <p className="text-xs text-gray-500 line-clamp-2 leading-relaxed">
                          {String(ep.findings_summary)}
                        </p>
                      ) : null}
                      {ep.tools_used ? (
                        <p className="text-xs text-indigo-500">Tools: {String(ep.tools_used)}</p>
                      ) : null}
                      {ep.procedure_used ? (
                        <p className="text-xs text-emerald-500">Procedure: {String(ep.procedure_used)}</p>
                      ) : null}
                    </div>
                  ))}
                  {(!episodic?.episodes || episodic.episodes.length === 0) && (
                    <div className="text-center py-12">
                      <Clock className="w-10 h-10 text-gray-200 mx-auto mb-3" />
                      <p className="text-sm text-gray-400">
                        No episodes yet. Start chatting to build episodic memory.
                      </p>
                    </div>
                  )}
                </div>
              </ScrollArea>
            </Card>

            <Card className="bg-white border-0 ring-1 ring-gray-100 p-6 rounded-2xl shadow-sm">
              <h3 className="text-sm font-bold text-gray-800 mb-4">
                State Changes
                <Badge className="ml-2 bg-amber-50 text-amber-600 border-amber-100 text-xs">
                  {episodic?.state_changes?.length || 0}
                </Badge>
              </h3>
              <ScrollArea className="h-[500px]">
                <div className="space-y-3">
                  {(episodic?.state_changes || []).map((sc: Record<string, unknown>, i: number) => (
                    <div key={i} className="p-4 rounded-xl bg-amber-50/40 border border-amber-100/60 space-y-2">
                      <span className="text-xs text-gray-400">{String(sc.timestamp || "").slice(0, 19)}</span>
                      <p className="text-sm text-amber-800 font-medium">
                        {String(sc.entity_type)}: {String(sc.entity_id)}
                      </p>
                      <div className="flex items-center gap-2 text-xs text-gray-600">
                        <span>{String(sc.field)}:</span>
                        <span className="font-mono bg-gray-100 px-1.5 py-0.5 rounded">{String(sc.old_value)}</span>
                        <ArrowRight className="w-3 h-3 text-gray-400" />
                        <span className="font-mono bg-amber-100 px-1.5 py-0.5 rounded text-amber-700">{String(sc.new_value)}</span>
                      </div>
                    </div>
                  ))}
                  {(!episodic?.state_changes || episodic.state_changes.length === 0) && (
                    <div className="text-center py-12">
                      <ArrowRight className="w-10 h-10 text-gray-200 mx-auto mb-3" />
                      <p className="text-sm text-gray-400">
                        No state changes detected yet.
                      </p>
                    </div>
                  )}
                </div>
              </ScrollArea>
            </Card>
          </div>
        </TabsContent>

        {/* Procedural Memory */}
        <TabsContent value="procedural" className="mt-6">
          <div className="grid grid-cols-2 gap-6">
            {Object.entries(procedural || {}).map(([name, proc]) => {
              const p = proc as Record<string, unknown>;
              const steps = (p.steps as Record<string, unknown>[]) || [];
              const history = (p.modification_history as Record<string, unknown>[]) || [];
              const execLog = (p.execution_log as Record<string, unknown>[]) || [];
              const totalRuns = execLog.length;
              const resolvedCount = execLog.filter((e) => e.outcome === "resolved").length;
              const resolvedRate = totalRuns > 0 ? Math.round((resolvedCount / totalRuns) * 100) : null;
              return (
                <Card key={name} className="bg-white border-0 ring-1 ring-gray-100 p-6 rounded-2xl shadow-sm">
                  <div className="flex items-center justify-between mb-3">
                    <h3 className="text-sm font-bold text-gray-800">{name}</h3>
                    <div className="flex items-center gap-2">
                      {totalRuns > 0 && (
                        <Badge className={`text-xs border ${resolvedRate !== null && resolvedRate >= 50 ? "bg-emerald-50 text-emerald-700 border-emerald-100" : "bg-amber-50 text-amber-700 border-amber-100"}`}>
                          {resolvedRate}% resolved · {totalRuns} runs
                        </Badge>
                      )}
                      <Badge className="bg-indigo-50 text-indigo-600 border-indigo-100 text-xs">v{String(p.version || 1)}</Badge>
                    </div>
                  </div>
                  <p className="text-xs text-gray-500 mb-4 leading-relaxed">{String(p.description || "")}</p>

                  <p className="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-2">Steps</p>
                  <div className="space-y-2 mb-4">
                    {steps.map((step: Record<string, unknown>, si: number) => (
                      <div key={si} className="flex gap-2.5 items-start text-xs p-2.5 bg-gray-50 rounded-lg">
                        <Badge variant="outline" className="text-[10px] shrink-0 border-gray-200 text-gray-500">
                          {String(step.action || "")}
                        </Badge>
                        <span className="text-gray-600 leading-relaxed">{String(step.description || "")}</span>
                      </div>
                    ))}
                  </div>

                  {history.length > 0 && (
                    <>
                      <p className="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-2">History</p>
                      {history.map((h: Record<string, unknown>, hi: number) => (
                        <p key={hi} className="text-xs text-indigo-500 mb-1">
                          v{String(h.from_version)} → v{String(h.to_version)}: {JSON.stringify(h.changes)}
                        </p>
                      ))}
                    </>
                  )}

                  {totalRuns > 0 && (
                    <>
                      <p className="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-2 mt-4">Recent Executions</p>
                      <div className="space-y-1.5">
                        {execLog.slice(-5).reverse().map((e: Record<string, unknown>, ei: number) => (
                          <div key={ei} className="flex items-center gap-2 text-xs">
                            <span className={`w-2 h-2 rounded-full shrink-0 ${e.outcome === "resolved" ? "bg-emerald-400" : e.outcome === "escalated" ? "bg-red-400" : "bg-amber-400"}`} />
                            <span className="text-gray-500">{String(e.timestamp || "").slice(0, 16)}</span>
                            <span className={`font-medium ${e.outcome === "resolved" ? "text-emerald-600" : e.outcome === "escalated" ? "text-red-600" : "text-amber-600"}`}>{String(e.outcome)}</span>
                          </div>
                        ))}
                      </div>
                    </>
                  )}

                  <p className="text-xs text-gray-400 mt-3">
                    Modified: {String(p.last_modified || "").slice(0, 19)}
                  </p>
                </Card>
              );
            })}
          </div>
        </TabsContent>

        {/* Semantic Memory */}
        <TabsContent value="semantic" className="mt-6">
          <div className="grid grid-cols-2 gap-6">
            {Object.entries(semantic || {}).map(([category, content]) => {
              const isModHistory = category === "modification_history" && Array.isArray(content);
              const title = category.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
              return (
                <Card key={category} className="bg-white border-0 ring-1 ring-gray-100 p-6 rounded-2xl shadow-sm">
                  <h3 className="text-sm font-bold text-gray-800 mb-4">
                    {title}
                    {isModHistory && (
                      <Badge className="ml-2 bg-amber-50 text-amber-600 border-amber-100 text-xs">
                        {(content as unknown[]).length} updates
                      </Badge>
                    )}
                  </h3>
                  <ScrollArea className="h-64">
                    {isModHistory ? (
                      <div className="space-y-3">
                        {(content as Record<string, unknown>[]).slice().reverse().map((entry, i) => (
                          <div key={i} className="p-3 rounded-xl bg-amber-50/40 border border-amber-100/60 space-y-1">
                            <div className="flex items-center gap-2">
                              <Badge className="text-[10px] bg-indigo-50 text-indigo-600 border-indigo-100">{String(entry.category || "")}</Badge>
                              <span className="text-xs font-semibold text-gray-700">{String(entry.key || "")}</span>
                              <span className="text-xs text-gray-400 ml-auto">{String(entry.timestamp || "").slice(0, 16)}</span>
                            </div>
                            <p className="text-xs text-gray-600 leading-relaxed">{String(entry.value || "")}</p>
                            {entry.reason ? (
                              <p className="text-xs text-amber-600 italic">Reason: {String(entry.reason)}</p>
                            ) : null}
                          </div>
                        ))}
                        {(content as unknown[]).length === 0 && (
                          <p className="text-xs text-gray-400 text-center py-6">No updates yet. Ask the agent to search for regulatory info to populate this.</p>
                        )}
                      </div>
                    ) : (
                      <pre className="text-xs text-gray-600 whitespace-pre-wrap leading-relaxed font-mono bg-gray-50 p-4 rounded-xl">
                        {typeof content === "string" ? content : JSON.stringify(content, null, 2)}
                      </pre>
                    )}
                  </ScrollArea>
                </Card>
              );
            })}
          </div>
        </TabsContent>
      </Tabs>
    </div>
  );
}
