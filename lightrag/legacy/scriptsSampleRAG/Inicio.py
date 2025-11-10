
import gradio as gr
from common.rag_pagoR2 import respuesta

gr.ChatInterface(respuesta).launch()
