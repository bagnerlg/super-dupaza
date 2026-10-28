-- SUPER DUPAZA 5.27.0
-- Optimización final PostgreSQL
-- Ejecutar UNA vez en Supabase SQL Editor.

BEGIN;

-- La reconciliación FULL_RECONCILE filtra loaded_refs por report_date.
-- Este índice evita escaneos completos a medida que crezca el histórico.
CREATE INDEX IF NOT EXISTS idx_loaded_refs_report_date
  ON super_dupaza.loaded_refs (report_date);

-- Actualiza estadísticas del planner después de la migración y los índices.
ANALYZE super_dupaza.loaded_refs;
ANALYZE super_dupaza.zas_orders;
ANALYZE super_dupaza.zas_order_lines;
ANALYZE super_dupaza.report_orders;
ANALYZE super_dupaza.sync_state;

COMMIT;
