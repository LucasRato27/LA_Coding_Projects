# Plan: Organizar `fitmax_mp_atrasada` no padrão dos projetos similares

O projeto `fitmax_mp_atrasada` já tem todos os arquivos relevantes, mas o nome do relatório gerado diverge do padrão estabelecido pelos outros projetos de análise. O ajuste principal é um rename de arquivo. A pasta `.serena/` em `mp_skp_project/` é o único lixo identificável na workspace.

## Passos

1. Renomear `root_cause_report.md` → `root_cause_summary.md`, alinhando com `ddal_mp_atrasada` e `lutestil_icp_causas_raiz`.

2. Atualizar a célula de geração de relatório no notebook `fitmax_relatorio.ipynb` para escrever em `root_cause_summary.md` ao invés de `root_cause_report.md`.

3. Excluir a pasta `mp_skp_project/.serena/` — artefato do assistente Serena, irrelevante para o projeto e não presente em nenhum outro projeto.

4. *(Opcional)* Criar `root_cause_summary.md` stub em `icp_causas_raiz/` — atualmente o único projeto sem README ou outputs, apenas um SQL solto.

## Considerações

1. **Padrão de referência:** `ddal_mp_atrasada` é o projeto mais completo; todos os outros são subconjuntos dele. Faz sentido adotá-lo como template?
2. **Notebook `fitmax_relatorio.ipynb`:** é exclusivo do fitmax — os outros projetos não têm notebook de automação. Devemos criar esse padrão nos demais, ou deixar o notebook como característica específica deste projeto?
3. **`icp_causas_raiz/`:** parece incompleto (só tem 1 SQL, sem README/CSV). Deve ser excluído ou completado?
