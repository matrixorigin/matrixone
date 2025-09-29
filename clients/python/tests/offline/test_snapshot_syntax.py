"""
Test MatrixOne snapshot syntax
"""

from matrixone import Client


def test_snapshot_syntax():
    """Test different snapshot syntax variations"""
    client = Client()

    try:
        # Connect to MatrixOne
        client.connect(host="localhost", port=6001, user="root", password="111", database="test")

        print("✓ Connected to MatrixOne successfully")

        # Create test table
        client.execute(
            """
            CREATE TABLE IF NOT EXISTS test_syntax (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100)
            )
        """
        )

        # Insert test data
        client.execute("INSERT INTO test_syntax (name) VALUES ('test')")

        print("✓ Test table created and data inserted")

        # Test different snapshot syntax variations
        syntax_variations = [
            "CREATE SNAPSHOT test_snapshot_001 TABLE test.test_syntax",
            "CREATE SNAPSHOT test_snapshot_001 OF TABLE test.test_syntax",
            "CREATE SNAPSHOT test_snapshot_001 FOR TABLE test.test_syntax",
            "CREATE SNAPSHOT test_snapshot_001 FROM TABLE test.test_syntax",
            "CREATE SNAPSHOT test_snapshot_001 CLONE TABLE test.test_syntax",
        ]

        for i, syntax in enumerate(syntax_variations):
            try:
                print(f"Testing syntax {i+1}: {syntax}")
                client.execute(syntax)
                print(f"✓ Syntax {i+1} worked!")

                # Try to drop the snapshot
                try:
                    client.execute("DROP SNAPSHOT test_snapshot_001")
                    print(f"✓ Snapshot dropped successfully")
                except Exception as e:
                    print(f"✗ Failed to drop snapshot: {e}")

                break

            except Exception as e:
                print(f"✗ Syntax {i+1} failed: {e}")

        # Test listing snapshots
        try:
            result = client.execute("SHOW SNAPSHOTS")
            print(f"✓ SHOW SNAPSHOTS worked: {result.fetchall()}")
        except Exception as e:
            print(f"✗ SHOW SNAPSHOTS failed: {e}")

        # Test other snapshot commands
        try:
            result = client.execute("SELECT * FROM mo_catalog.mo_snapshots")
            print(f"✓ mo_catalog.mo_snapshots query worked: {len(result.fetchall())} rows")
        except Exception as e:
            print(f"✗ mo_catalog.mo_snapshots query failed: {e}")

        # Clean up
        client.execute("DROP TABLE test_syntax")

    except Exception as e:
        print(f"✗ Test failed: {e}")
    finally:
        client.disconnect()


if __name__ == "__main__":
    test_snapshot_syntax()
