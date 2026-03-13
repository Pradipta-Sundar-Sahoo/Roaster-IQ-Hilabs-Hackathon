declare module "react-plotly.js" {
  import { Component } from "react";

  interface PlotParams {
    data: Plotly.Data[];
    layout?: Partial<Plotly.Layout>;
    config?: Partial<Plotly.Config>;
    style?: React.CSSProperties;
    className?: string;
    onInitialized?: (figure: { data: Plotly.Data[]; layout: Partial<Plotly.Layout> }) => void;
    onUpdate?: (figure: { data: Plotly.Data[]; layout: Partial<Plotly.Layout> }) => void;
  }

  export default class Plot extends Component<PlotParams> {}
}

declare namespace Plotly {
  interface Data {
    [key: string]: unknown;
  }
  interface Layout {
    [key: string]: unknown;
  }
  interface Config {
    [key: string]: unknown;
  }
}
