class Queries:
    def __init__(self):
        self.survey = """
        SELECT
          s.id AS survey_id
          , q.prompt
          , q.type
          , DATE(s.created_at) AS survey_day
        FROM surveys AS s
        LEFT JOIN survey_templates AS st ON s.survey_template_id = st.id
        LEFT JOIN questions AS q ON st.question_id = q.id
        WHERE s.organization_id = @organization_id
        """

        self.segment = """
        SELECT
          id AS segment_id
          , label AS segment_label
        FROM segments
        WHERE organization_id = @organization_id;
        """

        self.response = """
        SELECT
          id AS response_id
          , question_response_integer
          , IF(submitted_at IS NOT NULL, 1 , 0) AS submitted
          , survey_id
          , segment_id
        FROM responses 
        WHERE organization_id = @organization_id
        """

        self.filter = """
        SELECT
          id AS filter_id
          , label AS filter_label
        FROM filters
        WHERE organization_id = @organization_id
        """

        self.filter_apply = """
        SELECT 
          source_id AS response_id
          , filter_id 
        FROM filterships
        WHERE filter_id IN (@filter_ids)
          AND source_type = "Response"
        """
