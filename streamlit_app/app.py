"""Büyük Veri Analizi – Adım 7 Streamlit Dashboard
Asadabad İstasyonu Günlük İklim Verisi (1957–2010)
Veri Kaynağı: Gold Layer Delta Lake tabloları
"""

import os
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Climate Analytics Dashboard",
    page_icon="🌡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

DELTA_BASE = os.environ.get("DELTA_BASE", "/delta-lake/gold")
MONTH_NAMES = ["Oca", "Şub", "Mar", "Nis", "May", "Haz",
               "Tem", "Ağu", "Eyl", "Eki", "Kas", "Ara"]

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    [data-testid="stAppViewContainer"] { background: #0f1117; }
    [data-testid="stSidebar"]          { background: #161b25; }
    h1, h2, h3, h4                     { color: #e0e6f0; }

    .kpi-grid {
        display: grid;
        grid-template-columns: repeat(5, 1fr);
        gap: 12px;
        margin-bottom: 1.4rem;
    }
    .kpi-box {
        background: linear-gradient(135deg, #1a2340 0%, #1e3a5f 100%);
        border: 1px solid #2a4a7f;
        border-radius: 12px;
        padding: 1.1rem 1rem;
        text-align: center;
    }
    .kpi-value { font-size: 1.75rem; font-weight: 700; color: #5bc8f5; margin: 0; }
    .kpi-label { font-size: 0.78rem; color: #8ca0c0; margin: 0.25rem 0 0; }

    .badge {
        display: inline-block;
        background: #1e3a5f; color: #5bc8f5;
        border-radius: 20px; padding: 2px 10px;
        font-size: 0.75rem; margin: 2px;
    }
    .section-divider { border: none; border-top: 1px solid #2a3a55; margin: 1.2rem 0; }
</style>
""", unsafe_allow_html=True)


# ── Data loading ──────────────────────────────────────────────────────────────
@st.cache_data(show_spinner="Gold tabloları Delta Lake'ten yükleniyor…")
def load_gold_data():
    try:
        from deltalake import DeltaTable
        def read(path):
            return DeltaTable(path).to_pandas()
    except Exception:
        def read(path):
            return pd.read_parquet(path)

    model_pd   = read(f"{DELTA_BASE}/model_results")
    monthly_pd = read(f"{DELTA_BASE}/city_monthly_summary")
    monthly_pd = monthly_pd.sort_values(["year", "month"]).reset_index(drop=True)
    ml_pd      = read(f"{DELTA_BASE}/ml_features")

    # Sütun adlarını normalize et: model_results tablosunun adları farklı olabilir
    col_rename = {}
    for c in model_pd.columns:
        cl = c.lower().strip()
        if cl in ("r2", "r2_score", "r_squared", "r2score"):
            col_rename[c] = "R2"
        elif cl in ("rmse", "root_mean_squared_error"):
            col_rename[c] = "RMSE"
        elif cl in ("mae", "mean_absolute_error"):
            col_rename[c] = "MAE"
    if col_rename:
        model_pd = model_pd.rename(columns=col_rename)

    if "model_name" not in model_pd.columns:
        str_cols = model_pd.select_dtypes(include=["object", "string"]).columns.tolist()
        if str_cols:
            model_pd = model_pd.rename(columns={str_cols[0]: "model_name"})

    return model_pd, monthly_pd, ml_pd


try:
    model_pd, monthly_pd, ml_pd = load_gold_data()
except Exception as exc:
    st.error(f"Gold tabloları yüklenemedi: {exc}")
    st.info("Önce 05_feature_engineering.ipynb ve 06_ml_models.ipynb notebook'larını çalıştırın.")
    st.stop()


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🌍 Climate Dashboard")
    st.markdown("**Büyük Veri Analizi**  \nAdım 7 – Streamlit")
    st.divider()

    st.markdown("### Proje Bilgisi")
    st.markdown("📍 **İstasyon:** Asadabad, Afganistan")
    st.markdown("📅 **Dönem:** 1957 – 2010")
    st.markdown(f"📊 **Kayıt:** {len(ml_pd):,} satır")
    st.markdown(f"🤖 **Model:** {len(model_pd)} adet")

    if not model_pd.empty and "R2" in model_pd.columns and "model_name" in model_pd.columns:
        best_idx = model_pd["R2"].idxmax()
        best = model_pd.loc[best_idx]
        st.markdown(f"🏆 **En İyi:** {best['model_name']}  \n   R² = {best['R2']:.4f}")

    st.divider()
    st.markdown("### Medallion Mimarisi")
    st.markdown(
        '<span class="badge">🥉 Bronze</span>'
        '<span class="badge">🥈 Silver</span>'
        '<span class="badge">🥇 Gold ✓</span>',
        unsafe_allow_html=True,
    )
    st.caption("Dashboard yalnızca Gold Layer kullanır")
    st.divider()
    st.caption("Apache Kafka · Spark Streaming  \nDelta Lake · MLflow · Streamlit")
    with st.expander("🔍 Debug: Sütun adları"):
        st.write("model_results:", model_pd.columns.tolist())
        st.write("monthly:", monthly_pd.columns.tolist())
        st.write("ml_features:", ml_pd.columns.tolist())


# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_overview, tab_model, tab_temp, tab_precip, tab_ml = st.tabs([
    "🏠 Genel Bakış",
    "🤖 Model Performansı",
    "🌡️ Sıcaklık Analizi",
    "🌧️ Yağış Analizi",
    "🔬 ML Görüşleri",
])


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 0 – GENEL BAKIŞ
# ═══════════════════════════════════════════════════════════════════════════════
with tab_overview:
    st.title("🌍 Daily Climate Big Data – Analiz Dashboard")
    st.markdown(
        "**Asadabad İstasyonu, Afganistan (1957–2010)** &nbsp;|&nbsp; "
        "Medallion Mimarisi: Bronze → Silver → **Gold** &nbsp;|&nbsp; "
        "Apache Kafka + Spark Streaming + Delta Lake + MLflow",
        unsafe_allow_html=True,
    )
    st.markdown('<hr class="section-divider">', unsafe_allow_html=True)

    avg_temp  = ml_pd["avg_temp_c"].mean() if "avg_temp_c" in ml_pd.columns else 0
    best_r2   = model_pd["R2"].max() if (not model_pd.empty and "R2" in model_pd.columns) else 0
    best_name = (
        model_pd.loc[model_pd["R2"].idxmax(), "model_name"]
        if (not model_pd.empty and "R2" in model_pd.columns and "model_name" in model_pd.columns)
        else "-"
    )

    st.markdown(f"""
    <div class="kpi-grid">
      <div class="kpi-box"><p class="kpi-value">{len(ml_pd):,}</p><p class="kpi-label">Toplam Kayıt</p></div>
      <div class="kpi-box"><p class="kpi-value">53</p><p class="kpi-label">Yıl (1957–2010)</p></div>
      <div class="kpi-box"><p class="kpi-value">{avg_temp:.1f}°C</p><p class="kpi-label">Ort. Sıcaklık</p></div>
      <div class="kpi-box"><p class="kpi-value">{len(model_pd)}</p><p class="kpi-label">ML Modeli</p></div>
      <div class="kpi-box"><p class="kpi-value">{best_r2:.4f}</p><p class="kpi-label">En İyi R² ({best_name})</p></div>
    </div>
    """, unsafe_allow_html=True)

    col_l, col_r = st.columns([3, 2])

    with col_l:
        st.markdown("#### 📈 Yıllık Sıcaklık Trendi")
        yearly = (
            monthly_pd.groupby("year")
            .agg(avg_temp=("avg_temp_c", "mean"),
                 min_temp=("min_temp_c", "min"),
                 max_temp=("max_temp_c", "max"))
            .reset_index()
        )
        z = np.polyfit(yearly["year"], yearly["avg_temp"], 1)

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=pd.concat([yearly["year"], yearly["year"][::-1]]),
            y=pd.concat([yearly["max_temp"], yearly["min_temp"][::-1]]),
            fill="toself", fillcolor="rgba(91,200,245,0.1)",
            line_color="rgba(0,0,0,0)", name="Min–Max Aralığı",
        ))
        fig.add_trace(go.Scatter(
            x=yearly["year"], y=yearly["avg_temp"],
            mode="lines+markers", name="Yıllık Ort.",
            line=dict(color="#5bc8f5", width=2), marker=dict(size=4),
            hovertemplate="<b>%{x}</b><br>Ort: %{y:.1f}°C<extra></extra>",
        ))
        fig.add_trace(go.Scatter(
            x=yearly["year"], y=np.poly1d(z)(yearly["year"]),
            mode="lines", name=f"Trend ({z[0]:+.4f}°C/yıl)",
            line=dict(color="#ff6b6b", dash="dash", width=1.5),
        ))
        fig.update_layout(
            template="plotly_dark", height=370,
            margin=dict(l=0, r=0, t=30, b=0),
            xaxis_title="Yıl", yaxis_title="Sıcaklık (°C)",
            legend=dict(orientation="h", y=1.08),
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_r:
        st.markdown("#### 🤖 Model Sıralaması")
        if not model_pd.empty:
            disp = (
                model_pd[["model_name", "R2", "RMSE", "MAE"]]
                .sort_values("R2", ascending=False)
                .reset_index(drop=True)
            )
            disp.index += 1
            disp["R2"]   = disp["R2"].map("{:.4f}".format)
            disp["RMSE"] = disp["RMSE"].map("{:.4f}".format)
            disp["MAE"]  = disp["MAE"].map("{:.4f}".format)
            disp.columns = ["Model", "R²", "RMSE", "MAE"]
            st.dataframe(disp, use_container_width=True, height=220)

        st.markdown("#### 🌡️ Aylık Ortalama Sıcaklık")
        monthly_avg = monthly_pd.groupby("month")["avg_temp_c"].mean().reset_index()
        monthly_avg["ay"] = monthly_avg["month"].map(lambda x: MONTH_NAMES[x - 1])
        fig2 = px.bar(
            monthly_avg, x="ay", y="avg_temp_c",
            color="avg_temp_c", color_continuous_scale="RdBu_r",
            labels={"avg_temp_c": "°C", "ay": ""},
            template="plotly_dark",
        )
        fig2.update_layout(
            coloraxis_showscale=False, height=200,
            margin=dict(l=0, r=0, t=10, b=0),
        )
        st.plotly_chart(fig2, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1 – MODEL PERFORMANSI
# ═══════════════════════════════════════════════════════════════════════════════
with tab_model:
    st.title("🤖 Model Performans Karşılaştırması")
    st.caption("Kaynak: **gold/model_results** | 5 PySpark MLlib modeli, MLflow ile takip edildi")

    if model_pd.empty:
        st.warning("model_results tablosu boş – 06_ml_models.ipynb'yi çalıştırın.")
    else:
        best  = model_pd.loc[model_pd["R2"].idxmax()]
        worst = model_pd.loc[model_pd["RMSE"].idxmax()]

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("🏆 En İyi Model", best["model_name"])
        c2.metric("R² Score", f"{best['R2']:.4f}")
        c3.metric("RMSE", f"{best['RMSE']:.4f} °C",
                  delta=f"{worst['RMSE'] - best['RMSE']:.4f} vs worst",
                  delta_color="inverse")
        c4.metric("MAE", f"{best['MAE']:.4f} °C")

        st.markdown('<hr class="section-divider">', unsafe_allow_html=True)
        col_l, col_r = st.columns(2)

        with col_l:
            st.markdown("#### R² Score (yüksek = iyi)")
            mp_s = model_pd.sort_values("R2", ascending=True)
            fig = px.bar(
                mp_s, x="R2", y="model_name", orientation="h",
                color="R2", color_continuous_scale="viridis",
                text=mp_s["R2"].map("{:.4f}".format),
                template="plotly_dark",
                labels={"R2": "R² Score", "model_name": ""},
            )
            fig.update_traces(textposition="inside", insidetextanchor="middle")
            fig.update_layout(
                coloraxis_showscale=False, height=320,
                margin=dict(l=0, r=0, t=10, b=0),
                xaxis_range=[0.98, 1.0],
            )
            st.plotly_chart(fig, use_container_width=True)

        with col_r:
            st.markdown("#### RMSE & MAE (düşük = iyi)")
            fig = go.Figure()
            fig.add_trace(go.Bar(
                name="RMSE", x=model_pd["model_name"], y=model_pd["RMSE"],
                marker_color="#5bc8f5",
                text=model_pd["RMSE"].map("{:.4f}".format), textposition="outside",
            ))
            fig.add_trace(go.Bar(
                name="MAE", x=model_pd["model_name"], y=model_pd["MAE"],
                marker_color="#ff9f69",
                text=model_pd["MAE"].map("{:.4f}".format), textposition="outside",
            ))
            fig.update_layout(
                barmode="group", template="plotly_dark",
                height=320, margin=dict(l=0, r=0, t=10, b=0),
                yaxis_title="Hata (°C)",
                legend=dict(orientation="h", y=1.08),
                xaxis_tickangle=-20,
            )
            st.plotly_chart(fig, use_container_width=True)

        st.markdown("#### Radar Karşılaştırması (normalleştirilmiş)")
        mp = model_pd.copy()
        for col, higher_better in [("R2", True), ("RMSE", False), ("MAE", False)]:
            mn, mx = mp[col].min(), mp[col].max()
            if mx - mn < 1e-9:
                mp[f"{col}_n"] = 1.0
            elif higher_better:
                mp[f"{col}_n"] = (mp[col] - mn) / (mx - mn)
            else:
                mp[f"{col}_n"] = 1 - (mp[col] - mn) / (mx - mn)

        cats   = ["R²", "RMSE (inv)", "MAE (inv)"]
        colors = px.colors.qualitative.Set2
        radar  = go.Figure()
        for i, (_, row) in enumerate(mp.iterrows()):
            vals = [row["R2_n"], row["RMSE_n"], row["MAE_n"]]
            radar.add_trace(go.Scatterpolar(
                r=vals + [vals[0]], theta=cats + [cats[0]],
                fill="toself", name=row["model_name"],
                line_color=colors[i % len(colors)], opacity=0.7,
            ))
        radar.update_layout(
            polar=dict(radialaxis=dict(visible=True, range=[0, 1])),
            template="plotly_dark",
            height=380, margin=dict(l=40, r=40, t=20, b=20),
            legend=dict(orientation="h"),
        )
        st.plotly_chart(radar, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2 – SICAKLIK ANALİZİ
# ═══════════════════════════════════════════════════════════════════════════════
with tab_temp:
    st.title("🌡️ Sıcaklık Analizi")
    st.caption("Kaynak: **gold/city_monthly_summary** + **gold/ml_features**")

    year_min = int(monthly_pd["year"].min())
    year_max = int(monthly_pd["year"].max())
    yr_range = st.slider("Yıl Aralığı Seçin", year_min, year_max, (year_min, year_max))

    filtered = monthly_pd[monthly_pd["year"].between(*yr_range)]
    yearly = (
        filtered.groupby("year")
        .agg(avg_temp=("avg_temp_c", "mean"),
             min_temp=("min_temp_c", "min"),
             max_temp=("max_temp_c", "max"))
        .reset_index()
    )

    st.markdown("#### 📈 Yıllık Sıcaklık Trendi")
    z = np.polyfit(yearly["year"], yearly["avg_temp"], 1) if len(yearly) > 1 else [0, 0]
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=pd.concat([yearly["year"], yearly["year"][::-1]]),
        y=pd.concat([yearly["max_temp"], yearly["min_temp"][::-1]]),
        fill="toself", fillcolor="rgba(91,200,245,0.1)",
        line_color="rgba(0,0,0,0)", name="Min–Max Aralığı",
    ))
    fig.add_trace(go.Scatter(
        x=yearly["year"], y=yearly["avg_temp"],
        mode="lines+markers", name="Yıllık Ort.",
        line=dict(color="#5bc8f5", width=2), marker=dict(size=5),
    ))
    if len(yearly) > 1:
        fig.add_trace(go.Scatter(
            x=yearly["year"], y=np.poly1d(z)(yearly["year"]),
            mode="lines", name=f"Trend ({z[0]:+.4f}°C/yıl)",
            line=dict(color="#ff6b6b", dash="dash", width=1.5),
        ))
    fig.update_layout(
        template="plotly_dark", height=360,
        margin=dict(l=0, r=0, t=10, b=0),
        xaxis_title="Yıl", yaxis_title="Sıcaklık (°C)",
        legend=dict(orientation="h", y=1.08),
    )
    st.plotly_chart(fig, use_container_width=True)

    col_l, col_r = st.columns(2)

    with col_l:
        st.markdown("#### 📅 Aylık Mevsimsellik (hata çubuklu)")
        m_agg = monthly_pd.groupby("month")["avg_temp_c"].agg(["mean", "std"]).reset_index()
        m_agg["ay"] = m_agg["month"].map(lambda x: MONTH_NAMES[x - 1])
        fig = go.Figure(go.Bar(
            x=m_agg["ay"], y=m_agg["mean"],
            error_y=dict(type="data", array=m_agg["std"].values, visible=True),
            marker=dict(color=m_agg["mean"], colorscale="RdBu_r", showscale=True,
                        colorbar=dict(title="°C", thickness=10)),
            hovertemplate="<b>%{x}</b><br>Ort: %{y:.1f}°C<extra></extra>",
        ))
        fig.update_layout(
            template="plotly_dark", height=310,
            margin=dict(l=0, r=50, t=10, b=0),
            xaxis_title="Ay", yaxis_title="°C",
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_r:
        st.markdown("#### 🥧 Mevsim Dağılımı")
        if "month" in ml_pd.columns:
            def m2s(m):
                if m in [12, 1, 2]:  return "Kış ❄️"
                elif m in [3, 4, 5]: return "İlkbahar 🌸"
                elif m in [6, 7, 8]: return "Yaz ☀️"
                return "Sonbahar 🍂"

            ml2 = ml_pd.copy()
            ml2["_season"] = ml2["month"].apply(m2s)
            sc = ml2["_season"].value_counts().reset_index()
            sc.columns = ["Mevsim", "Sayı"]
            fig = px.pie(sc, values="Sayı", names="Mevsim",
                         color_discrete_sequence=["#4ECDC4", "#45B7D1", "#FF6B6B", "#FFA07A"],
                         hole=0.4, template="plotly_dark")
            fig.update_traces(textposition="outside", textinfo="percent+label")
            fig.update_layout(height=310, margin=dict(l=0, r=0, t=10, b=0), showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

    st.markdown("#### 🌡️ Sıcaklık Dağılımı (Histogram)")
    if "avg_temp_c" in ml_pd.columns:
        td = ml_pd["avg_temp_c"].dropna()
        fig = px.histogram(td, nbins=60, color_discrete_sequence=["#5bc8f5"],
                           template="plotly_dark",
                           labels={"value": "Sıcaklık (°C)", "count": "Gün Sayısı"})
        fig.add_vline(x=td.mean(), line_color="white",
                      annotation_text=f"Ort: {td.mean():.1f}°C", annotation_position="top right")
        fig.add_vline(x=td.median(), line_color="#ffa07a", line_dash="dash",
                      annotation_text=f"Medyan: {td.median():.1f}°C", annotation_position="top left")
        fig.add_vline(x=0, line_color="cyan", line_dash="dot",
                      annotation_text="0°C (donma)", annotation_position="bottom right")
        fig.update_layout(height=300, margin=dict(l=0, r=0, t=30, b=0), showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("#### 🗓️ Yıl × Ay Sıcaklık Heatmap'i")
    pivot = monthly_pd.pivot_table(values="avg_temp_c", index="year", columns="month", aggfunc="mean")
    pivot.columns = MONTH_NAMES
    fig = px.imshow(pivot, color_continuous_scale="RdBu_r", aspect="auto",
                    text_auto=".0f", template="plotly_dark",
                    labels=dict(color="°C"))
    fig.update_layout(height=460, margin=dict(l=0, r=0, t=10, b=0),
                      xaxis_title="Ay", yaxis_title="Yıl",
                      coloraxis_colorbar=dict(title="°C", thickness=12))
    st.plotly_chart(fig, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 3 – YAĞIŞ ANALİZİ
# ═══════════════════════════════════════════════════════════════════════════════
with tab_precip:
    st.title("🌧️ Yağış Analizi")
    st.caption("Kaynak: **gold/city_monthly_summary** + **gold/ml_features**")

    col_l, col_r = st.columns(2)

    with col_l:
        st.markdown("#### Aylık Yağış ve Yağışlı Gün")
        mp_agg = monthly_pd.groupby("month").agg(
            total_precip=("total_precipitation_mm", "mean"),
            rainy_days=("rainy_days", "mean"),
        ).reset_index()
        mp_agg["ay"] = mp_agg["month"].map(lambda x: MONTH_NAMES[x - 1])

        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(go.Bar(
            x=mp_agg["ay"], y=mp_agg["total_precip"],
            name="Ort. Toplam Yağış (mm)", marker_color="#5bc8f5", opacity=0.8,
        ), secondary_y=False)
        fig.add_trace(go.Scatter(
            x=mp_agg["ay"], y=mp_agg["rainy_days"],
            name="Ort. Yağışlı Gün", mode="lines+markers",
            line=dict(color="#ffa07a", width=2), marker=dict(size=7),
        ), secondary_y=True)
        fig.update_yaxes(title_text="Toplam Yağış (mm)", secondary_y=False)
        fig.update_yaxes(title_text="Yağışlı Gün", secondary_y=True)
        fig.update_layout(
            template="plotly_dark", height=340,
            margin=dict(l=0, r=50, t=10, b=0),
            legend=dict(orientation="h", y=1.08),
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_r:
        st.markdown("#### Yağış Kategorisi Dağılımı")
        if "precipitation_category" in ml_pd.columns:
            cat_order = ["none", "light", "moderate", "heavy"]
            vals_in   = [c for c in cat_order if c in ml_pd["precipitation_category"].values]
            pc = (
                ml_pd["precipitation_category"]
                .value_counts()
                .reindex(vals_in)
                .reset_index()
            )
            pc.columns = ["Kategori", "Sayı"]
            pc["Yüzde"] = (pc["Sayı"] / len(ml_pd) * 100).round(1)
            color_map = {"none": "#95A5A6", "light": "#3498DB", "moderate": "#2980B9", "heavy": "#1A5276"}
            fig = px.bar(
                pc, x="Kategori", y="Sayı",
                color="Kategori", color_discrete_map=color_map,
                text="Yüzde", template="plotly_dark",
                labels={"Sayı": "Gün Sayısı"},
            )
            fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
            fig.update_layout(height=340, margin=dict(l=0, r=0, t=10, b=0), showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("'precipitation_category' sütunu ml_features tablosunda bulunamadı.")

    st.markdown("#### 🗓️ Yıl × Ay Yağış Heatmap'i (mm)")
    pivot_p = monthly_pd.pivot_table(
        values="total_precipitation_mm", index="year", columns="month", aggfunc="mean"
    )
    pivot_p.columns = MONTH_NAMES
    fig = px.imshow(pivot_p, color_continuous_scale="Blues", aspect="auto",
                    text_auto=".0f", template="plotly_dark",
                    labels=dict(color="mm"))
    fig.update_layout(height=460, margin=dict(l=0, r=0, t=10, b=0),
                      xaxis_title="Ay", yaxis_title="Yıl",
                      coloraxis_colorbar=dict(title="mm", thickness=12))
    st.plotly_chart(fig, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 4 – ML ANALİZİ
# ═══════════════════════════════════════════════════════════════════════════════
with tab_ml:
    st.title("🔬 ML Görüşleri")
    st.caption("Kaynak: **gold/ml_features** | scikit-learn ile yeniden eğitim (RF + LR)")

    NUMERIC = [
        "max_temp_c", "min_temp_c", "dew_point_c", "humidity_pct",
        "cloud_cover_pct", "wind_speed_kmh", "visibility_km",
        "precipitation_mm", "sea_level_pressure_hpa",
        "temp_lag_1", "temp_rolling_avg_7d", "temp_diff_from_yesterday",
    ]
    TARGET    = "avg_temp_c"
    CAT       = ["precipitation_category"]
    avail_num = [c for c in NUMERIC if c in ml_pd.columns]
    avail_cat = [c for c in CAT    if c in ml_pd.columns]
    all_feats = avail_num + avail_cat

    @st.cache_data(show_spinner="sklearn RF + LR eğitiliyor…")
    def fit_models(X: pd.DataFrame, y: pd.Series):
        from sklearn.impute import SimpleImputer
        from sklearn.preprocessing import OrdinalEncoder
        from sklearn.pipeline import Pipeline
        from sklearn.compose import ColumnTransformer
        from sklearn.ensemble import RandomForestRegressor
        from sklearn.linear_model import LinearRegression
        from sklearn.model_selection import train_test_split
        from sklearn.metrics import r2_score, mean_squared_error

        num_pre = Pipeline([("imp", SimpleImputer(strategy="median"))])
        cat_pre = Pipeline([
            ("imp", SimpleImputer(strategy="constant", fill_value="none")),
            ("enc", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1)),
        ])
        prep = ColumnTransformer([
            ("num", num_pre, avail_num),
            ("cat", cat_pre, avail_cat),
        ]) if avail_cat else ColumnTransformer([("num", num_pre, avail_num)])

        X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.2, random_state=42)

        rf_pipe = Pipeline([("prep", prep), ("rf", RandomForestRegressor(n_estimators=50, max_depth=8, random_state=42))])
        rf_pipe.fit(X_tr, y_tr)
        y_rf = rf_pipe.predict(X_te)

        lr_pipe = Pipeline([("prep", prep), ("lr", LinearRegression())])
        lr_pipe.fit(X_tr, y_tr)
        y_lr = lr_pipe.predict(X_te)

        return dict(
            feat_names  = avail_num + avail_cat,
            importances = rf_pipe.named_steps["rf"].feature_importances_,
            y_test      = y_te.values,
            y_rf        = y_rf,
            y_lr        = y_lr,
            rf_r2       = r2_score(y_te, y_rf),
            rf_rmse     = float(np.sqrt(mean_squared_error(y_te, y_rf))),
            lr_r2       = r2_score(y_te, y_lr),
            lr_rmse     = float(np.sqrt(mean_squared_error(y_te, y_lr))),
        )

    df_ml = ml_pd[all_feats + [TARGET]].dropna(subset=[TARGET])
    res   = fit_models(df_ml[all_feats], df_ml[TARGET])

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("RF R²",   f"{res['rf_r2']:.4f}")
    c2.metric("RF RMSE", f"{res['rf_rmse']:.4f} °C")
    c3.metric("LR R²",   f"{res['lr_r2']:.4f}")
    c4.metric("LR RMSE", f"{res['lr_rmse']:.4f} °C")

    st.markdown('<hr class="section-divider">', unsafe_allow_html=True)
    col_l, col_r = st.columns(2)

    with col_l:
        st.markdown("#### 📊 Feature Importance (RF)")
        imp_df = (
            pd.DataFrame({"Özellik": res["feat_names"], "Önem": res["importances"]})
            .sort_values("Önem", ascending=True)
        )
        fig = px.bar(
            imp_df, x="Önem", y="Özellik", orientation="h",
            color="Önem", color_continuous_scale="RdYlGn",
            text=imp_df["Önem"].map("{:.4f}".format),
            template="plotly_dark",
            labels={"Önem": "Önem Skoru", "Özellik": ""},
        )
        fig.update_traces(textposition="inside", insidetextanchor="middle")
        fig.update_layout(coloraxis_showscale=False, height=420, margin=dict(l=0, r=0, t=10, b=0))
        fig.add_vline(x=res["importances"].mean(), line_dash="dash", line_color="red",
                      annotation_text=f'Ort: {res["importances"].mean():.4f}')
        st.plotly_chart(fig, use_container_width=True)

    with col_r:
        st.markdown("#### 🎯 Aktual vs Tahmin (LR)")
        rng = np.random.RandomState(42)
        idx = rng.choice(len(res["y_test"]), min(2000, len(res["y_test"])), replace=False)
        y_a, y_p = res["y_test"][idx], res["y_lr"][idx]

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=y_a, y=y_p, mode="markers",
            marker=dict(size=4, color="#5bc8f5", opacity=0.35),
            name="Tahminler",
            hovertemplate="Gerçek: %{x:.1f}°C<br>Tahmin: %{y:.1f}°C<extra></extra>",
        ))
        lims = [min(y_a.min(), y_p.min()), max(y_a.max(), y_p.max())]
        fig.add_trace(go.Scatter(
            x=lims, y=lims, mode="lines",
            line=dict(color="#ff6b6b", dash="dash", width=1.5),
            name="Mükemmel Tahmin",
        ))
        fig.update_layout(
            template="plotly_dark", height=420,
            margin=dict(l=0, r=0, t=10, b=0),
            xaxis_title="Gerçek Sıcaklık (°C)", yaxis_title="Tahmin (°C)",
            legend=dict(orientation="h", y=1.08),
        )
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("#### 📐 Rezidü Dağılımı (LR)")
    residuals = res["y_lr"] - res["y_test"]
    fig = px.histogram(residuals, nbins=60, color_discrete_sequence=["#5bc8f5"],
                       template="plotly_dark",
                       labels={"value": "Rezidü (Tahmin − Gerçek) °C", "count": "Frekans"})
    fig.add_vline(x=0, line_color="red", annotation_text="Sıfır Rezidü", annotation_position="top right")
    fig.add_vline(x=residuals.mean(), line_color="#ffa07a", line_dash="dash",
                  annotation_text=f"Ort: {residuals.mean():.3f}", annotation_position="top left")
    fig.update_layout(height=280, margin=dict(l=0, r=0, t=30, b=0), showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("#### 🔗 Korelasyon Matrisi")
    corr_cols  = ["avg_temp_c", "humidity_pct", "cloud_cover_pct",
                  "wind_speed_kmh", "precipitation_mm", "visibility_km", "dew_point_c"]
    avail_corr = [c for c in corr_cols if c in ml_pd.columns]
    corr = ml_pd[avail_corr].dropna().corr()
    fig  = px.imshow(corr, color_continuous_scale="RdBu_r", zmin=-1, zmax=1,
                     text_auto=".2f", aspect="auto", template="plotly_dark",
                     labels=dict(color="r"))
    fig.update_layout(height=400, margin=dict(l=0, r=0, t=10, b=0),
                      coloraxis_colorbar=dict(title="r", thickness=12))
    st.plotly_chart(fig, use_container_width=True)
