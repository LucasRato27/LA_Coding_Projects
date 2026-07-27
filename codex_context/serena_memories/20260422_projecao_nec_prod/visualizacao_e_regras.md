Visualizações e regras atuais no NRP_Projecao.ipynb:
- Seção 7.2 com gráficos de consolidação:
  1) Heatmap de new_need por product_name x mês (cenário base)
  2) Top 15 product_name por new_need acumulada (comparativo de cenários)
  3) Treemap artigo -> cor por production_need
  4) Treemap product_name -> cor por new_need
  5) Barras empilhadas de production_need por cor ao longo dos meses
- Treemaps usam formatação legível K/M com %{value:.3s} em texto e hover.

Regra de filtro por artigo/cor em df_proj_color_article:
- valid_modal_colors define combinações permitidas por article_name específico.
- Para article_name == 'Modal': remove cores bloqueadas listadas em valid_modal_colors.
- Para article_name presente como chave em valid_modal_colors: mantém apenas cores permitidas.

Exportação:
- nrp_monthly_{today}.csv
- nrp_weekly_{today}.csv
- nrp_summary_{today}.csv
- nrp_proj_color_article_{today}.csv
