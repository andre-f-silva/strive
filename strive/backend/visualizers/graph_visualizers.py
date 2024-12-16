import holoviews as hv
import hvplot as df
import networkx as nx
import plotly.graph_objs as go
from bokeh.models import Plot, Range1d, MultiLine, Circle, HoverTool
from bokeh.plotting import from_networkx
from bokeh.plotting import show
hv.extension('bokeh')
from pyvis.network import Network


def plotly_visualizer(dag):
    G = dag
    pos = nx.spectral_layout(G)
    nx.set_node_attributes(G, pos, 'pos')
    # Create a layout for the plot
    layout = go.Layout(
        title="Interactive NetworkX Graph",
        showlegend=False,
        hovermode="closest",
        margin=dict(b=20, l=5, r=5, t=40),
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False))
    # Create a Plotly figure
    fig = go.Figure(data=[], layout=layout)
    # Add the edges to the plot
    for edge in G.edges():
        x0, y0 = G.nodes[edge[0]]['pos']
        x1, y1 = G.nodes[edge[1]]['pos']
        trace = go.Scatter(x=tuple([x0, x1, None]), y=tuple([y0, y1, None]), mode='lines', line=dict(width=1),
                           hoverinfo='none')
        fig.add_trace(trace)
    # Add the nodes to the plot
    for node in G.nodes():
        x, y = G.nodes[node]['pos']
        try:
            node_text = node.label
        except Exception as err:
            node_text = node

        trace = go.Scatter(x=[x], y=[y], mode='markers', marker=dict(symbol='circle-dot', size=18, line=dict(width=1)),
                           text=node_text, hoverinfo='text', opacity=0.8)
        fig.add_trace(trace)
    # Show the plot
    fig.show()


def bokeh_visualizer(dag):

    # Create a graph
    G = dag

    # Create a Bokeh plot
    plot = Plot(width=400, height=400, x_range=Range1d(-1.1, 1.1), y_range=Range1d(-1.1, 1.1))

    # Create a Bokeh graph from the NetworkX graph
    graph = from_networkx(G, nx.spring_layout, scale=1, center=(0, 0), )

    # Customize the graph
    graph.node_renderer.glyph = Circle(size=20, fill_color='gray')
    graph.edge_renderer.glyph = MultiLine(line_alpha=0.8, line_width=1)
    graph.inspection_policy = HoverTool(always_active=True)
    graph.selection_policy = None

    # Add the graph to the plot and show it
    plot.renderers.append(graph)
    show(plot)


def holoviews_visualizer(dag):

    # Create a graph
    G = dag

    # Create a Holoviews graph from the NetworkX graph
    graph = hv.Graph.from_networkx(G, nx.spring_layout)

    # Customize the graph
    graph.opts(
        node_color='gray',
        edge_line_alpha=0.8,
        node_size=20,
        edge_line_width=1,
        inspection_policy='hover',
        selection_policy=None)

    # Show the graph
    #graph.show()
    # using hvplot here to create a holoviews plot
    # could have also just used holoviews itself
    plot = df.hvplot(kind='scatter', x='col1', y='col2')

    # use show() from bokeh
    show(hv.render(plot))

    pass


def pyvis_visualizer(dag):

    # Create a graph
    G = dag

    # Create a Pyvis network from the NetworkX graph
    net = Network(height='500px', width='500px', bgcolor='white', font_color='black')
    net.from_nx(G)

    # Customize the network
    net.show_buttons()
    net.toggle_physics(False)

    # Show the network
    file_name = 'example.html'
    net.show(file_name)

    pass

