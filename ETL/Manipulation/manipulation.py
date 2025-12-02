import pandas as pd
from typing import Optional, List
from Logging.logger import ETLLogger


class DataManipulation:
    """Handles data transformation, cleaning, and enrichment."""

    def __init__(self, logger: Optional[ETLLogger] = None):
        self.logger = logger or ETLLogger(name="DataManipulation")

    # ==================== MAIN ORCHESTRATION ====================

    def manipulate(
        self, df: pd.DataFrame, operations: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Main orchestration function to apply all manipulation operations in sequence.

        Args:
            df: Input DataFrame.
            operations: List of operations to apply. If None, applies all in default order:
                       ['clean', 'standardize', 'compute', 'sort'].
                       Available: ['clean', 'standardize', 'compute', 'sort', 'filter'].

        Returns:
            Manipulated DataFrame.
        """
        if operations is None:
            operations = ["clean", "standardize", "compute", "sort"]

        self.logger.info("MANIPULATION PIPELINE")

        for i, op in enumerate(operations, 1):
            self.logger.info(f"[{i}/{len(operations)}] Applying: {op.upper()}")

            if op == "clean":
                df = self.clean_data(df)
            elif op == "standardize":
                df = self.standardize_columns(df)
            elif op == "compute":
                df = self.add_computed_fields(df)
            elif op == "sort":
                df = self.sort_by_value(df)
            elif op == "filter":
                df = self.filter_by_value(df, min_value=0)
            else:
                self.logger.warning(f"Unknown operation: {op}")

        self.logger.info(f"MANIPULATION COMPLETE: {len(df)} records")
        return df

    # ==================== DATA CLEANING ====================

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean data: remove nulls, duplicates, trim whitespace.

        Args:
            df: Input DataFrame.

        Returns:
            Cleaned DataFrame.
        """
        self.logger.info("Cleaning data...")

        # Remove duplicate rows
        original_count = len(df)
        df = df.drop_duplicates()
        removed_dupes = original_count - len(df)

        if removed_dupes > 0:
            self.logger.info(f"Removed {removed_dupes} duplicate rows")

        # Trim whitespace from string columns
        for col in df.select_dtypes(include="object").columns:
            df[col] = df[col].str.strip()

        # Remove rows where key fields are null
        key_fields = ["cusip", "nameOfIssuer", "value"]
        for field in key_fields:
            if field in df.columns:
                df = df[df[field].notna()]

        self.logger.info(f"Clean: {len(df)} records remaining")
        return df

    # ==================== STANDARDIZATION ====================

    def standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize column names and data types.

        Args:
            df: Input DataFrame.

        Returns:
            Standardized DataFrame.
        """
        self.logger.info("Standardizing columns...")

        # Convert value columns to numeric
        numeric_cols = [
            "value",
            "shares",
            "voting_sole",
            "voting_shared",
            "voting_none",
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Standardize string columns to uppercase where appropriate
        string_cols = ["cusip", "share_type", "put_call", "discretion"]
        for col in string_cols:
            if col in df.columns:
                df[col] = df[col].str.upper()

        self.logger.info(f"Standardized: {df.shape[1]} columns")
        return df

    # ==================== COMPUTED FIELDS ====================

    def add_computed_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add computed/derived fields.

        Args:
            df: Input DataFrame.

        Returns:
            DataFrame with computed fields.
        """
        self.logger.info("Adding computed fields...")

        # Add total voting power
        if all(
            col in df.columns for col in ["voting_sole", "voting_shared", "voting_none"]
        ):
            df["voting_total"] = (
                df["voting_sole"].fillna(0)
                + df["voting_shared"].fillna(0)
                + df["voting_none"].fillna(0)
            )

        # Add percentage fields
        if "value" in df.columns:
            total_value = df["value"].sum()
            if total_value > 0:
                df["value_pct"] = (df["value"] / total_value * 100).round(2)

        # Add data quality flag
        df["is_complete"] = (df.notna().sum(axis=1) >= df.shape[1] * 0.8).astype(int)

        self.logger.info(f"Added computed fields")
        return df

    # ==================== FILTERING ====================

    def filter_by_value(self, df: pd.DataFrame, min_value: float = 0) -> pd.DataFrame:
        """Filter holdings by minimum value threshold."""
        if "value" in df.columns:
            original_count = len(df)
            df = df[df["value"] >= min_value]
            filtered_count = original_count - len(df)
            self.logger.info(
                f"Filtered: removed {filtered_count} holdings below {min_value}"
            )
        return df

    # ==================== SORTING ====================

    def sort_by_value(self, df: pd.DataFrame, descending: bool = True) -> pd.DataFrame:
        """Sort holdings by value."""
        if "value" in df.columns:
            df = df.sort_values("value", ascending=not descending)
            self.logger.info(f"Sorted by value ({('DESC' if descending else 'ASC')})")
        return df
