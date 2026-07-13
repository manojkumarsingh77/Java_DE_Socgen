import { useState } from "react";
import { Prism as SyntaxHighlighter } from "react-syntax-highlighter";
import { vscDarkPlus } from "react-syntax-highlighter/dist/esm/styles/prism";
import { Copy, Check } from "lucide-react";

interface Props {
  code: string;
  lang?: string;
  maxH?: string;
}

export default function CodeBlock({ code, lang = "java", maxH = "520px" }: Props) {
  const [copied, setCopied] = useState(false);

  const handleCopy = () => {
    navigator.clipboard.writeText(code).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    });
  };

  return (
    <div className="relative group rounded-xl overflow-hidden border border-slate-700 shadow-xl">
      <button
        onClick={handleCopy}
        className="absolute top-3 right-3 z-10 flex items-center gap-1.5 px-2.5 py-1.5 
                   rounded-lg bg-slate-700/80 hover:bg-slate-600 text-slate-300 
                   hover:text-white text-xs font-medium transition-all duration-150
                   opacity-0 group-hover:opacity-100"
      >
        {copied ? <Check size={13} /> : <Copy size={13} />}
        {copied ? "Copied!" : "Copy"}
      </button>
      <div style={{ maxHeight: maxH, overflow: "auto" }}>
        <SyntaxHighlighter
          language={lang}
          style={vscDarkPlus}
          showLineNumbers
          wrapLines
          customStyle={{
            margin: 0,
            borderRadius: 0,
            fontSize: "0.78rem",
            lineHeight: "1.55",
            background: "#0d1117",
          }}
          lineNumberStyle={{ color: "#4a5568", minWidth: "2.8em" }}
        >
          {code}
        </SyntaxHighlighter>
      </div>
    </div>
  );
}
