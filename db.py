import gradio as gr
from app.main import run_user_query, make_human_answer


def respond(message, history):
    sql, rows = run_user_query(message)
    answer = make_human_answer(message, rows, sql)
    return answer


def main():
    demo = gr.ChatInterface(respond, title="mac-ai")
    demo.launch()


if __name__ == "__main__":
    main()
