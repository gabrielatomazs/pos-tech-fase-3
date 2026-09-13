import os
import textwrap
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

COLOR_PRIMARY_VIVID = "#FF5722" # Laranja vivo / coral vibrante (destaques)
COLOR_BLUE_VIVID = "#007ACC" # Azul elétrico moderno
COLOR_NEUTRAL_BAR = "#90A4AE" # Cinza neutro elegante para barras secundárias
COLOR_TEXT_DARK = "#1E293B" # Texto escuro para máxima legibilidade
COLOR_MUTED = "#64748B" # Elementos secundários

COR_MASCULINO = "#007ACC" # Azul elétrico
COR_FEMININO = "#E91E63" # Rosa magenta vivo
CORES_VIVAS = ["#90A4AE", "#007ACC", "#FF5722", "#9C27B0", "#009688"]


def set_aws_style():
    plt.rcParams.update({
        "figure.facecolor": "#FFFFFF",
        "axes.facecolor": "#FFFFFF",
        "axes.edgecolor": "#CBD5E1",
        "axes.linewidth": 1.0,
        "axes.labelcolor": COLOR_TEXT_DARK,
        "axes.titlecolor": COLOR_TEXT_DARK,
        "text.color": COLOR_TEXT_DARK,
        "xtick.color": COLOR_TEXT_DARK,
        "ytick.color": COLOR_TEXT_DARK,
        "font.family": "sans-serif",
        "font.sans-serif": ["DejaVu Sans", "Arial", "Helvetica", "sans-serif"],
        "font.size": 11,
        "axes.titlesize": 14,
        "axes.titleweight": "bold",
        "axes.labelsize": 11,
        "axes.labelweight": "medium",
        "axes.grid": False,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "figure.dpi": 200,
    })


def formata_reais(x, _pos=None):
    return f"R$ {x:,.0f}".replace(",", ".")


def formata_milhar(x, _pos=None):
    return f"{x:,.0f}".replace(",", ".")


def salvar(fig, nome_arquivo):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    caminho = os.path.join(OUTPUT_DIR, nome_arquivo)
    fig.tight_layout()
    fig.savefig(caminho, dpi=300, bbox_inches="tight", facecolor=fig.get_facecolor(), edgecolor='none')
    plt.close(fig)
    print(f"✅ Gráfico salvo em: {caminho}")


# --------------------- Pergunta 1 - Gráfico 1: Cargos mais comuns ---------------------
def grafico_cargos_mais_comuns():
    df = pd.read_csv(os.path.join(DATA_DIR, "q1_cargos.csv"))
    mapa_normalizacao = {
        "Engenheiro de Dados/Arquiteto de Dados/Data Engineer/Data Architect":
            "Engenheiro de Dados/Data Engineer/Data Architect",
    }
    df["cargo_atual"] = df["cargo_atual"].replace(mapa_normalizacao)
    agg = df.groupby("cargo_atual", as_index=False)["total"].sum()
    agg["percentual"] = (agg["total"] / agg["total"].sum() * 100).round(1)
    agg = agg.sort_values("total", ascending=True).tail(12)

    cores_barras = [COLOR_NEUTRAL_BAR] * (len(agg) - 1) + [COLOR_BLUE_VIVID]

    set_aws_style()
    fig, ax = plt.subplots(figsize=(10, 6.5))
    bars = ax.barh(agg["cargo_atual"], agg["total"], color=cores_barras, height=0.65, edgecolor="none")
    
    max_val = agg["total"].max()
    for bar, pct in zip(bars, agg["percentual"]):
        ax.text(bar.get_width() + max_val * 0.015, bar.get_y() + bar.get_height() / 2,
                f"{pct}%", va="center", ha="left", fontsize=10, fontweight="bold", color=COLOR_TEXT_DARK)

    ax.set_title("Cargos Mais Comuns no Mercado de Dados", pad=15)
    ax.set_xlabel("Número de Respondentes")
    ax.set_xlim(0, max_val * 1.22)
    salvar(fig, "q1_cargos_mais_comuns.png")


# ---------------- Pergunta 1 - Gráfico 2: Distribuição de senioridade por edição ----------------
def grafico_senioridade_por_ano():
    caminho = os.path.join(DATA_DIR, "q1_senioridade_ano.csv")
    if not os.path.exists(caminho):
        return

    df = pd.read_csv(caminho)
    ordem_senioridade = ["Júnior", "Pleno", "Sênior", "Especialista/Staff+"]
    pivot = df.pivot(index="ano_pesquisa", columns="senioridade", values="percentual")
    cols_presentes = [c for c in ordem_senioridade if c in pivot.columns]
    pivot = pivot[cols_presentes].fillna(0)

    set_aws_style()
    fig, ax = plt.subplots(figsize=(9, 6))
    pivot.plot(kind="bar", ax=ax, color=CORES_VIVAS[:len(cols_presentes)], width=0.7, edgecolor="none")
    
    ax.set_title("Evolução da Distribuição de Senioridade por Edição", pad=15)
    ax.set_ylabel("Percentual dos Respondentes (%)")
    ax.set_xlabel("")
    plt.xticks(rotation=0)
    ax.legend(title="Senioridade", frameon=False, bbox_to_anchor=(0.5, -0.15), loc="upper center", ncol=4, fontsize=10, title_fontsize=11)
    
    max_val = pivot.values.max()
    if pd.isna(max_val) or max_val <= 0:
        max_val = 100.0
    ax.set_ylim(0, max_val * 1.25)
    
    salvar(fig, "q1_senioridade_por_ano.png")

# ---------------- Pergunta 1 - Gráfico 3: Distribuição por setor ----------------
def grafico_distribuicao_setor():
    df = pd.read_csv(os.path.join(DATA_DIR, "q1_setor.csv"))
    df = df.sort_values("total", ascending=True)

    cores_barras = [COLOR_NEUTRAL_BAR] * (len(df) - 1) + [COLOR_BLUE_VIVID]

    set_aws_style()
    fig, ax = plt.subplots(figsize=(10, 6.5))
    bars = ax.barh(df["setor"], df["total"], color=cores_barras, height=0.65, edgecolor="none")

    max_val = df["total"].max()
    for bar, pct in zip(bars, df["percentual"]):
        ax.text(bar.get_width() + max_val * 0.015, bar.get_y() + bar.get_height() / 2,
                f"{pct}%", va="center", ha="left", fontsize=10, fontweight="bold", color=COLOR_TEXT_DARK)

    ax.set_title("Distribuição de Profissionais por Setor da Empresa", pad=15)
    ax.set_xlabel("Número de Respondentes")
    ax.set_xlim(0, max_val * 1.22)
    salvar(fig, "q1_distribuicao_setor.png")

# -------------- Pergunta 2 - Gráfico 1: Top cargo + senioridade por salário --------------
def grafico_top_perfis_valorizados():
    df = pd.read_csv(os.path.join(DATA_DIR, "q2_cargo_senioridade.csv"))

    df["cargo_atual"] = df["cargo_atual"].str.split("/").str[0].str.strip()

    df["soma_ponderada"] = df["salario_medio"] * df["total"]
    agg = df.groupby(["cargo_atual", "senioridade"], as_index=False).agg(
        soma_ponderada=("soma_ponderada", "sum"),
        total=("total", "sum"),
    )
    agg["salario_medio"] = agg["soma_ponderada"] / agg["total"]
    agg["label"] = agg["cargo_atual"] + " (" + agg["senioridade"] + ")"
    agg = agg.sort_values("salario_medio", ascending=True).tail(12)

    cores_barras = [COLOR_NEUTRAL_BAR] * (len(agg) - 1) + [COLOR_PRIMARY_VIVID]

    set_aws_style()
    fig, ax = plt.subplots(figsize=(10.5, 7))
    bars = ax.barh(agg["label"], agg["salario_medio"], color=cores_barras, height=0.65, edgecolor="none")

    max_sal = agg["salario_medio"].max()
    for bar in bars:
        val = bar.get_width()
        ax.text(val + max_sal * 0.01, bar.get_y() + bar.get_height() / 2,
                formata_reais(val), va="center", ha="left", fontsize=9.5, fontweight="bold", color=COLOR_TEXT_DARK)

    ax.set_title("Perfis Profissionais Mais Valorizados (Salário Médio)", pad=15)
    ax.set_xlabel("Salário Médio (R$)")
    ax.xaxis.set_major_locator(mticker.MaxNLocator(nbins=6))
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(formata_reais))
    ax.set_xlim(0, max_sal * 1.28)
    salvar(fig, "q2_top_perfis_valorizados.png")


# ------------- Pergunta 2 - Gráfico 2: Impacto da qtd. de linguagens no salário -------------
def grafico_impacto_linguagens():
    df = pd.read_csv(os.path.join(DATA_DIR, "q2_qtd_linguagens.csv"))
    df = df.dropna(subset=["qtd_linguagens"])
    df["qtd_linguagens"] = df["qtd_linguagens"].astype(int)
    df = df.sort_values("qtd_linguagens")

    labels_x = [f"{n} lang." if n > 1 else f"{n} lang." for n in df["qtd_linguagens"]]

    set_aws_style()
    fig, ax = plt.subplots(figsize=(9, 6))
    bars = ax.bar(labels_x, df["salario_medio"], color=COLOR_BLUE_VIVID, width=0.6, edgecolor="none")

    for bar, total in zip(bars, df["total"]):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 400,
            f"{formata_milhar(total)} pes.",
            ha="center", va="bottom", fontsize=9, fontweight="bold", color=COLOR_TEXT_DARK,
        )

    ax.set_title("Impacto do Domínio de Múltiplas Linguagens no Salário Médio", pad=25)
    ax.text(
        0.5, 1.03,
        "Base avaliada: SQL, Python, R, C/C++/C#, Julia, VB/VBA, Scala e Rust",
        transform=ax.transAxes, ha="center", fontsize=9.5, color=COLOR_MUTED, style="italic"
    )
    ax.set_xlabel("Quantidade de Linguagens Dominadas")
    ax.set_ylabel("Salário Médio (R$)")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(formata_reais))
    ax.set_ylim(0, df["salario_medio"].max() * 1.25)
    salvar(fig, "q2_impacto_linguagens.png")

# ---------------- Pergunta 1/6 - Gráfico 4: Modelo de trabalho atual ----------------
def grafico_modelo_trabalho():
    df = pd.read_csv(os.path.join(DATA_DIR, "q1_modelo_trabalho.csv"))

    mapa_rotulos = {
        "Trabalho presencial": "100% presencial",
        "Modelo 100% presencial": "100% presencial",
        "Trabalho remoto": "100% remoto",
        "Modelo 100% remoto": "100% remoto",
    }
    df["modelo_curto"] = df["modelo_trabalho_atual"].map(mapa_rotulos).fillna(
        df["modelo_trabalho_atual"].str.slice(0, 30) + "..."
    )

    pivot = df.pivot_table(index="ano_pesquisa", columns="modelo_curto", values="percentual", aggfunc="sum").fillna(0)

    set_aws_style()
    fig, ax = plt.subplots(figsize=(10, 6.5))
    pivot.plot(kind="bar", ax=ax, color=CORES_VIVAS[:len(pivot.columns)], width=0.7, edgecolor="none")

    ax.set_title("Modelo de Trabalho Atual por Edição da Pesquisa", pad=15)
    ax.set_ylabel("Percentual dos Respondentes (%)")
    ax.set_xlabel("")
    ax.tick_params(axis='x', rotation=0)
    ax.legend(title="Modelo de trabalho", frameon=False, bbox_to_anchor=(0.5, -0.18), loc="upper center", ncol=2, fontsize=9, title_fontsize=10)

    max_val = pivot.values.max()
    ax.set_ylim(0, max_val * 1.25 if max_val > 0 else 100)
    salvar(fig, "q1_modelo_trabalho.png")
    
# -------------- Pergunta 3 - Gráfico 1: Representatividade de gênero por edição --------------
def grafico_representatividade_genero():
    df = pd.read_csv(os.path.join(DATA_DIR, "q3_genero_ano.csv"))
    pivot = df.pivot(index="ano_pesquisa", columns="genero", values="percentual")
    ordem = ["Masculino", "Feminino", "Outro", "Prefiro não informar"]
    ordem_presente = [c for c in ordem if c in pivot.columns]
    pivot = pivot[ordem_presente]
    cores = [COR_MASCULINO, COR_FEMININO, "#78909C", "#CFD8DC"]

    set_aws_style()
    fig, ax = plt.subplots(figsize=(8.5, 6))
    pivot.plot(kind="bar", stacked=True, ax=ax, color=cores[:len(ordem_presente)], width=0.55, edgecolor="none")

    for i, ano in enumerate(pivot.index):
        acumulado = 0
        for genero in ordem_presente:
            valor = pivot.loc[ano, genero]
            if genero in ("Masculino", "Feminino") and valor > 5:
                ax.text(i, acumulado + valor / 2, f"{valor:.1f}%", ha="center", va="center",
                        fontsize=9.5, color="white", fontweight="bold")
            acumulado += valor

    ax.set_title("Evolução da Representatividade de Gênero nas Pesquisas", pad=15)
    ax.set_ylabel("Distribuição (%)")
    ax.set_xlabel("")
    ax.set_ylim(0, 105)
    plt.xticks(rotation=0)
    ax.legend(title="Gênero", frameon=False, bbox_to_anchor=(0.5, -0.15), loc="upper center", ncol=4, fontsize=10)
    salvar(fig, "q3_representatividade_genero.png")


# ----------------- Pergunta 3 - Gráfico 2: Gap salarial por gênero e senioridade -----------------
def grafico_gap_salarial_genero():
    df = pd.read_csv(os.path.join(DATA_DIR, "q3_gap_genero_senioridade.csv"))
    pivot = df.pivot(index="senioridade", columns="genero", values="salario_medio")
    ordem_senioridade = ["Júnior", "Pleno", "Sênior", "Especialista/Staff+"]
    pivot = pivot.reindex([s for s in ordem_senioridade if s in pivot.index])

    set_aws_style()
    fig, ax = plt.subplots(figsize=(9, 6))
    x = range(len(pivot.index))
    width = 0.35

    ax.bar([i - width / 2 for i in x], pivot["Masculino"], width, label="Masculino", color=COR_MASCULINO, edgecolor="none")
    ax.bar([i + width / 2 for i in x], pivot["Feminino"], width, label="Feminino", color=COR_FEMININO, edgecolor="none")

    for i, senioridade in enumerate(pivot.index):
        masc = pivot.loc[senioridade, "Masculino"]
        fem = pivot.loc[senioridade, "Feminino"]
        gap_pct = (masc - fem) / masc * 100
        y_max = max(masc, fem)
        ax.text(i, y_max + y_max * 0.02, f"Gap: {gap_pct:.1f}%",
                 ha="center", va="bottom", fontsize=9, color=COLOR_TEXT_DARK, fontweight="bold")

    ax.set_xticks(list(x))
    ax.set_xticklabels(pivot.index)
    ax.set_title("Gap Salarial por Gênero (Controlado por Senioridade)", pad=15)
    ax.set_ylabel("Salário Médio (R$)")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(formata_reais))
    ax.legend(frameon=False, fontsize=10, loc="upper right")
    ax.set_ylim(0, pivot.values.max() * 1.25)
    salvar(fig, "q3_gap_salarial_genero.png")


# --------------------- Pergunta 4 - Gráfico: Tecnologias mais adotadas ---------------------
def grafico_tecnologias_mais_adotadas():
    df = pd.read_csv(os.path.join(DATA_DIR, "q4_tecnologias.csv"))
    df = df.sort_values("total_usuarios", ascending=True)

    cores_barras = [COLOR_NEUTRAL_BAR] * (len(df) - 1) + [COLOR_BLUE_VIVID]

    set_aws_style()
    fig, ax = plt.subplots(figsize=(10, 6.5))
    bars = ax.barh(df["tecnologia"], df["total_usuarios"], color=cores_barras, height=0.65, edgecolor="none")
    
    max_val = df["total_usuarios"].max()
    for bar in bars:
        val = bar.get_width()
        ax.text(val + max_val * 0.01, bar.get_y() + bar.get_height() / 2,
                 formata_milhar(val), va="center", ha="left", fontsize=9.5, fontweight="bold", color=COLOR_TEXT_DARK)

    ax.set_title("Tecnologias e Ferramentas Mais Adotadas no Mercado", pad=15)
    ax.set_xlabel("Número de Profissionais Usuários")
    ax.set_xlim(0, max_val * 1.22)
    salvar(fig, "q4_tecnologias_mais_adotadas.png")


# --------------------- Pergunta 5 - Gráfico: Adoção e impacto de IA ---------------------
def grafico_adocao_ia():
    caminho = os.path.join(DATA_DIR, "q5_adocao_ia.csv")
    if not os.path.exists(caminho):
        return

    df = pd.read_csv(caminho)
    
    mapa_rotulos_curtos = {
        "Sim, está entre nossas principais prioridades para os próximos 2-4 anos (com discussões de iniciativas e orçamentos de curto a médio prazo).": 
            "Prioridade alta (2-4 anos)",
        "Sim, é nossa principal prioridade como empresa (com foco executivo significativo e alocação de orçamento relevante).": 
            "Principal prioridade estratégica",
        "Não é uma iniciativa que estamos focando e não tem sido uma prioridade.": 
            "Não é prioridade / Sem foco",
        "Mais ou menos... É uma das várias iniciativas que estamos impulsionando, mas não é uma prioridade (tratam-se de iniciativas isoladas e com pouco foco).": 
            "Iniciativa isolada (Baixa prioridade)",
        "Mais ou menos... É uma das várias iniciativas que estamos impulsionando, mas não é uma prioridade (iniciativas isoladas e pouco foco).": 
            "Iniciativa isolada (Baixa prioridade)",
        "Não sei opinar sobre esse assunto.": 
            "Não sabe / Não opinou"
    }

    if "ia_prioridade_empresa" in df.columns:
        df["rotulo_curto"] = df["ia_prioridade_empresa"].map(mapa_rotulos_curtos).fillna(df["ia_prioridade_empresa"])
    else:
        df["rotulo_curto"] = "Categoria"

    if "total" in df.columns and "salario_medio" in df.columns:
        df["soma_ponderada"] = df["salario_medio"] * df["percentual"]
        agg = df.groupby("rotulo_curto", as_index=False).agg(
            percentual=("percentual", "sum"),
            soma_ponderada=("soma_ponderada", "sum")
        )
        agg["salario_medio"] = agg["soma_ponderada"] / agg["percentual"]
    else:
        agg = df.groupby("rotulo_curto", as_index=False).agg(
            percentual=("percentual", "sum"),
            salario_medio=("salario_medio", "mean")
        )

    agg = agg.sort_values("percentual", ascending=True)

    cores_barras = [COLOR_NEUTRAL_BAR] * (len(agg) - 1) + [COLOR_PRIMARY_VIVID]

    set_aws_style()
    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.barh(agg["rotulo_curto"], agg["percentual"], color=cores_barras, height=0.6, edgecolor="none")

    max_pct = agg["percentual"].max()
    if pd.isna(max_pct) or max_pct <= 0:
        max_pct = 30.0

    for bar, salario, pct in zip(bars, agg["salario_medio"], agg["percentual"]):
        salario_str = formata_reais(salario) if not pd.isna(salario) else "R$ 0"
        texto = f"{pct:.1f}%  |  Média: {salario_str}"
        ax.text(bar.get_width() + max_pct * 0.015, bar.get_y() + bar.get_height() / 2,
                 texto, va="center", ha="left", fontsize=10, color=COLOR_TEXT_DARK, fontweight="bold")

    ax.set_title("Prioridade de IA nas Empresas x Salário Médio", pad=15)
    ax.set_xlabel("Adoção / Prioridade (% dos Respondentes)")
    ax.set_xlim(0, max_pct * 1.62)
    
    salvar(fig, "q5_adocao_impacto_ia.png")


# --------------------- Pergunta 6 - Gráfico: Diferenças regionais ---------------------
def grafico_diferencas_regionais():
    df = pd.read_csv(os.path.join(DATA_DIR, "q6_regiao.csv"))
    df = df.sort_values("salario_medio", ascending=True)

    cores_barras = [COLOR_NEUTRAL_BAR] * (len(df) - 1) + [COLOR_PRIMARY_VIVID]

    set_aws_style()
    fig, ax = plt.subplots(figsize=(9, 6))
    bars = ax.barh(df["regiao_onde_mora"], df["salario_medio"], color=cores_barras, height=0.6, edgecolor="none")
    
    max_sal = df["salario_medio"].max()
    for bar in bars:
        val = bar.get_width()
        ax.text(val + max_sal * 0.01, bar.get_y() + bar.get_height() / 2,
                formata_reais(val), va="center", ha="left", fontsize=9.5, fontweight="bold", color=COLOR_TEXT_DARK)

    ax.set_title("Panorama de Salário Médio por Região do País", pad=15)
    ax.set_xlabel("Salário Médio (R$)")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(formata_reais))
    ax.set_xlim(0, max_sal * 1.25)
    salvar(fig, "q6_diferencas_regionais.png")

def gerar_graficos():
    print(" Iniciando geração dos gráficos com paleta viva e limpa...")
    
    grafico_cargos_mais_comuns()
    grafico_senioridade_por_ano()
    grafico_distribuicao_setor()      
    grafico_modelo_trabalho()         
    grafico_top_perfis_valorizados()
    grafico_impacto_linguagens()
    grafico_representatividade_genero()
    grafico_gap_salarial_genero()
    grafico_tecnologias_mais_adotadas()
    grafico_adocao_ia()
    grafico_diferencas_regionais()

    print(f"\n Todos os gráficos vivos e limpos foram salvos em: {OUTPUT_DIR}/")

if __name__ == "__main__":
    gerar_graficos()