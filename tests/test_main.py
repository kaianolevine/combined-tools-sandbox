from your_package import main


def test_main_runs(capsys):
    main.main()
    captured = capsys.readouterr()
    assert captured.out == ""
