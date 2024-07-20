{
    source ~/.brianward92rc
    echo "Creating conda environment..."

    conda create --name mtga python=3.10 black cvxpy jupyter keras matplotlib numpy pandas scikit-learn scipy seaborn tensorflow
    if [ $? -eq 0 ]; then

        if [ $? -eq 0 ]; then
            echo "DONE"
        else
            handle_error
        fi
    else
        handle_error
    fi
} || handle_error
