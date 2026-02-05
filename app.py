import streamlit as st
from dotenv import load_dotenv
import psycopg2
import os
import pandas as pd
from scripts.channel_constraint import apply_diversity_constraint
import plotly.express as px


@st.cache_resource
def load_model():
    from sentence_transformers import SentenceTransformer

    return SentenceTransformer("sentence-transformers/all-MiniLM-L12-v2")


@st.cache_resource
def load_db_connection():
    load_dotenv()
    POSTGRES_USER = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    POSTGRES_DB = os.getenv("POSTGRES_DB")
    POSTGRES_HOST = "localhost"

    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )
    conn.autocommit = True
    cursor = conn.cursor()
    return conn, cursor


conn, cursor = load_db_connection()


@st.cache_data
def get_thresholds(_conn, _cursor):
    try:
        query = """select percentile_cont(0.9) within group (order by relative_score) as percentile_90,
                percentile_cont(0.75) within group (order by relative_score) as percentile_75
                from relative_performance"""

        _cursor.execute(query)
        result = _cursor.fetchall()
        return result[0][0], result[0][1]
    except Exception as e:
        _conn.rollback()
        print(f"Error: {e}")


if "selected_channel" not in st.session_state:
    st.session_state.selected_channel = None


mode = st.sidebar.radio("Select A View", ["Search for videos", "Dashboard"])

if mode == "Search for videos":
    st.title("ContentIQ")
    search_term = st.text_input("Search for videos", key="search_term")

    if search_term:
        model = load_model()
        query_embedding = model.encode(search_term)

        highly_recommended_threshold, recommended_threshold = get_thresholds(
            conn, cursor
        )
        try:
            query = """
                    SELECT v.channel_id, v.video_id, c.channel_handle, v.title, 
                    v.view_count, v.like_count, v.comment_count, r.relative_score
                    FROM videos v
                    JOIN channels c
                    ON v.channel_id = c.channel_id
                    JOIN relative_performance r
                    ON r.video_id = v.video_id
                    ORDER BY v.embedding <=> %s::vector
                    LIMIT 50"""
            distance_df = pd.read_sql(query, conn, params=(query_embedding.tolist(),))
            df_w_constraint = apply_diversity_constraint(distance_df)
            for row in df_w_constraint.itertuples():
                left, right = st.columns([0.5, 0.5])
                with left:
                    st.image(f"https://img.youtube.com/vi/{row.video_id}/hqdefault.jpg")
                with right:
                    url = f"https://www.youtube.com/watch?v={row.video_id}"
                    link_html = f'<a href="{url}">{row.title}</a>'
                    st.markdown(f"### {link_html}", unsafe_allow_html=True)
                    st.write("##### channel: ", row.channel_handle)
                    st.write("View: ", row.view_count)
                    st.write("Like: ", row.like_count)
                    st.write("Comment: ", row.comment_count)
                    if row.relative_score >= highly_recommended_threshold:
                        st.badge("Highly Recommended", color="yellow")
                    elif row.relative_score >= recommended_threshold:
                        st.badge("Recommended", color="orange")
        except Exception as e:
            conn.rollback()
            st.write(f"Error: {e}")
elif mode == "Dashboard":
    st.title("Dashboard")

    query = """
        SELECT c.channel_handle, e.smoothed_engagement_rate
        FROM channels c
        JOIN engagement_rate e
        ON c.channel_id = e.channel_id
        """
    stats_df = pd.read_sql(query, conn)
    channel_stats = (
        stats_df.groupby("channel_handle")["smoothed_engagement_rate"]
        .agg(["mean", "std"])
        .round(3)
        .reset_index()
    )
    channel_stats["coefficient_of_variance"] = round(
        channel_stats["std"] / channel_stats["mean"], 3
    )
    median_engagement = channel_stats["mean"].median()
    # Normalize standard deviation for comparison
    median_cv = channel_stats["coefficient_of_variance"].median()

    def get_quadrant(row):
        """Return one of the four quadrants based on the ranges of mean and coefficient_of_variance."""
        if (
            row["mean"] <= median_engagement
            and row["coefficient_of_variance"] <= median_cv
        ):
            return "Consistent & Weak"
        elif (
            row["mean"] <= median_engagement
            and row["coefficient_of_variance"] > median_cv
        ):
            return "Inconsistent & Weak"
        elif (
            row["mean"] > median_engagement
            and row["coefficient_of_variance"] <= median_cv
        ):
            return "Consistent & Strong"
        else:
            return "Inconsistent & Strong"

    channel_stats["quadrant"] = channel_stats.apply(get_quadrant, axis=1)

    # Add selectbox for selecting a channel
    selected_channel = st.sidebar.selectbox(
        "Select a channel",
        options=[None] + list(channel_stats["channel_handle"].sort_values()),
        format_func=lambda x: "All channels" if x is None else x,
    )

    # 1. Create scatter plot
    scatter_fig = px.scatter(
        channel_stats,
        x="mean",
        y="coefficient_of_variance",
        hover_name="channel_handle",
        labels={
            "mean": "Average Engagement Rate",
            "coefficient_of_variance": "Consistency (CV)",
        },
        title="Channel Performance: Engagement vs Consistency",
        color="quadrant",
    )
    scatter_fig.update_layout(
        title_x=0.17,
        yaxis=dict(showline=True, showgrid=False, linewidth=0.2),
        xaxis=dict(showline=True, linewidth=0.2),
    )
    scatter_fig.add_hline(
        median_cv, line_dash="dash", line_color="orange", line_width=0.3
    )
    scatter_fig.add_vline(
        median_engagement, line_dash="dash", line_color="orange", line_width=0.3
    )
    scatter_fig.add_annotation(
        x=0.01,
        y=0.99,
        xref="paper",
        yref="paper",
        text="Inconsistent & Weak",
        showarrow=False,
        xanchor="left",
        yanchor="top",
        font=dict(size=15, color="gray"),
    )
    scatter_fig.add_annotation(
        x=0.01,
        y=0.01,
        xref="paper",
        yref="paper",
        text="Consistent & Weak",
        showarrow=False,
        xanchor="left",
        yanchor="bottom",
        font=dict(size=15, color="gray"),
    )
    scatter_fig.add_annotation(
        x=0.99,
        y=0.01,
        xref="paper",
        yref="paper",
        text="Consistent & Strong",
        showarrow=False,
        xanchor="right",
        yanchor="middle",
        font=dict(size=15, color="gray"),
    )
    scatter_fig.add_annotation(
        x=0.99,
        y=0.99,
        xref="paper",
        yref="paper",
        text="Inconsistent & Strong",
        showarrow=False,
        xanchor="right",
        yanchor="top",
        font=dict(size=15, color="gray"),
    )
    st.plotly_chart(scatter_fig, on_select="rerun")

    # 2. Create bar chart
    channel_stats_sorted = channel_stats.sort_values("mean", ascending=True)
    bar_fig = px.bar(
        channel_stats_sorted,
        x="mean",
        y="channel_handle",
        orientation="h",
        title="Average Engagement Rate by YouTube Channel",
        color="mean",
        color_continuous_scale="Blues",
        height=600,
    )
    bar_fig.update_yaxes(type="category", dtick=1)
    bar_fig.update_layout(title_x=0.25, coloraxis_showscale=False)
    st.plotly_chart(bar_fig, on_select="rerun")

    # Build video-level plots for selected channel
    if selected_channel:
        # 1. engagement vs video age scatter
        eng_vs_age_query = """
                            SELECT e.smoothed_engagement_rate, 
                            DATE_TRUNC('day', v.published_at) AS publish_date
                            FROM channels c
                            JOIN engagement_rate e
                            ON c.channel_id = e.channel_id
                            JOIN videos v
                            ON v.video_id = e.video_id
                            WHERE c.channel_handle = %s
                            """
        eng_vs_age_df = pd.read_sql(eng_vs_age_query, conn, params=(selected_channel,))
        eng_vs_age_scatter_fig = px.scatter(
            eng_vs_age_df,
            x="publish_date",
            y="smoothed_engagement_rate",
            labels={
                "publish_date": "Publish Date",
                "smoothed_engagement_rate": "Bayesian Smoothed Engagement Rate",
            },
            title=f"Engagement vs Video Age: {selected_channel}",
            color="smoothed_engagement_rate",
            color_continuous_scale="Purples",
        )
        eng_vs_age_scatter_fig.update_traces(marker=dict(opacity=0.8, size=5))
        eng_vs_age_scatter_fig.update_layout(
            title_x=0.35, title_y=0.95, coloraxis_showscale=False
        )
        st.plotly_chart(eng_vs_age_scatter_fig)

        # 2. relative score distribution histogram
        relative_score_query = """
                                SELECT r.video_id, r.relative_score
                                FROM relative_performance r
                                JOIN channels c
                                ON r.channel_id = c.channel_id
                                WHERE c.channel_handle = %s
                                """
        relative_score_df = pd.read_sql(
            relative_score_query, conn, params=(selected_channel,)
        )
        relative_score_fig = px.histogram(
            relative_score_df,
            x="relative_score",
            title=f"Relative Score Distribution: {selected_channel}",
            nbins=30,
            labels={"relative_score": "Relative Score"},
        )
        relative_score_fig.update_layout(title_x=0.35)
        relative_score_fig.update_traces(
            marker_color="rgba(145,210,255,1.000)",
            marker_line_color="white",
            marker_line_width=0.5,
        )
        st.plotly_chart(relative_score_fig)

        # 3. engagement trends by publish date
        eng_rate_by_month_query = """
                                SELECT DATE_TRUNC('month', v.published_at) AS year_month,
                                AVG(e.smoothed_engagement_rate) AS avg_engagement_rate
                                FROM videos v
                                JOIN engagement_rate e
                                ON v.video_id = e.video_id
                                JOIN channels c
                                ON v.channel_id = c.channel_id
                                WHERE c.channel_handle = %s
                                GROUP BY DATE_TRUNC('month', v.published_at)
                                ORDER BY year_month
                                """
        eng_rate_by_month_df = pd.read_sql(
            eng_rate_by_month_query, conn, params=(selected_channel,)
        )
        eng_rate_by_month_fig = px.line(
            eng_rate_by_month_df,
            x="year_month",
            y="avg_engagement_rate",
            title=f"Average Engagement Rate Trend By Month: {selected_channel}",
            labels={
                "year_month": "Publish Date",
                "avg_engagement_rate": "Average Engagement Rate",
            },
        )
        eng_rate_by_month_fig.update_layout(title_x=0.25)
        eng_rate_by_month_fig.update_traces(line_color="#C0C327")
        st.plotly_chart(eng_rate_by_month_fig)
