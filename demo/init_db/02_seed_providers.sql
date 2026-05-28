-- Seed provider catalog
-- NPI-005 (Suspect Clinic) is the fraudulent provider in the demo
INSERT INTO providers (npi, name, specialty, status) VALUES
    ('NPI-001', 'Cardio Clinic SP',   'CARDIOLOGY',    'ACTIVE'),
    ('NPI-002', 'Ortho Hospital',     'ORTHOPEDICS',   'ACTIVE'),
    ('NPI-003', 'MedLab Diagnostics', 'LABORATORY',    'ACTIVE'),
    ('NPI-004', 'PhysioPlus',         'PHYSIOTHERAPY', 'ACTIVE'),
    ('NPI-005', 'Suspect Clinic',     'CARDIOLOGY',    'ACTIVE')
ON CONFLICT (npi) DO NOTHING;

-- Seed a few members
INSERT INTO members (member_id, plan_id, status) VALUES
    ('MBR-1001', 'PLAN-GOLD',   'ACTIVE'),
    ('MBR-1002', 'PLAN-SILVER', 'ACTIVE'),
    ('MBR-1003', 'PLAN-GOLD',   'ACTIVE'),
    ('MBR-1004', 'PLAN-BASIC',  'ACTIVE'),
    ('MBR-1005', 'PLAN-SILVER', 'ACTIVE')
ON CONFLICT (member_id) DO NOTHING;
