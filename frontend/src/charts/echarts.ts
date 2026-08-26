import {
  HeatmapChart,
  LineChart,
} from 'echarts/charts'
import {
  GridComponent,
  LegendComponent,
  TooltipComponent,
  VisualMapComponent,
} from 'echarts/components'
import {
  init,
  use as registerECharts,
} from 'echarts/core'
import {
  CanvasRenderer,
} from 'echarts/renderers'


registerECharts([
  LineChart,
  HeatmapChart,
  GridComponent,
  LegendComponent,
  TooltipComponent,
  VisualMapComponent,
  CanvasRenderer,
])


export { init }
