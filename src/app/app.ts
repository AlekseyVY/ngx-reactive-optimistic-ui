import { AsyncPipe } from "@angular/common";
import { ChangeDetectionStrategy, Component } from "@angular/core";
import { takeUntilDestroyed } from "@angular/core/rxjs-interop";
import { catchError, concatMap, EMPTY, ignoreElements, map, merge, scan, shareReplay, startWith, Subject, switchMap, tap, timer, withLatestFrom } from "rxjs";


@Component({
    selector: 'app-root',
    templateUrl: './app.html',
    styleUrls: ['./app.css'],
    imports: [AsyncPipe],
    changeDetection: ChangeDetectionStrategy.OnPush
})
export class App {
    public update$ = new Subject<number>();
    private errorState$ = new Subject<string>();
    private correction$ = new Subject<number>();
    private delta$ = merge(this.update$, this.correction$);

    public count$ = this.delta$.pipe(
        startWith(0),
        scan((acc, delta) => acc + delta, 0),
        shareReplay(1)
    )

    private serverSync$ = this.update$.pipe(
        withLatestFrom(this.count$),
        concatMap(([delta, state]) => {
            return this.fakeApiSave(state).pipe(
                ignoreElements(),
                catchError((err: Error) => {
                    this.errorState$.next(err.message);
                    this.correction$.next(-delta);
                    return EMPTY;
                })
            )
        })
    )

    public error$ = this.errorState$.asObservable().pipe(
        switchMap((err) => timer(1000).pipe(map(() => null), startWith(err))) 
    )

    constructor() {
        this.serverSync$.pipe(takeUntilDestroyed()).subscribe()
    }

    private fakeApiSave(delta: number) {
        const isError = Math.random() > 0.5;
        return timer(500).pipe(
            tap(() => {
                if(isError) throw new Error('Ошибка сохранения!');
                console.log(`Значение: ${delta} сохранено успешно!`)
            })
        )
    }
}